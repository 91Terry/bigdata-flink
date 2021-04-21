package com.terry.realtime.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.jersey.core.util.StringIgnoreCaseKeyComparator;
import com.terry.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Properties;

//从kafka的ods层读取日志数据，根据不同日志类型进行分流，将分流后的数据写回答kafka的DWD主题中。
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //1、基本环境准备
        //创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度（和kafka分区数设置相同）
        env.setParallelism(4);

//        //检查点相关设置
//        //开启检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        //设置状态后端(内存/文件系统/RockDB)
//        env.setStateBackend(new FsStateBackend("hdfs://47.92.54.9:8020/gmall/checkpoint"));
//        //设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
//        //设置操作HDFS的用户
//        System.setProperty("HADOOP_USER_NAME","terry");

        //2、从kafka的ods_base_log中读取数据
        //声明消费主题和消费者组
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        //通过工具类获取KafkaSource
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //通过kafkaSource读取kafka的数据，并且封装为一个流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3、将字符串转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });

        //jsonObjDS.print("jsonObjDS");

        //4、对状态进行修复
        //按照mid进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        //对分组后的数据进行处理
        SingleOutputStreamOperator<JSONObject> jsonObjWithFlagDS = keyedDS
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    //定义状态
                    ValueState<String> firstVisitDateState;
                    //定义时间格式化工具类
                    SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDateState", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        //获取状态标记
                        String isNew = value.getJSONObject("common").getString("is_new");
                        //处理状态是1的标记
                        if ("1".equals(isNew)) {
                            //获取当前设备的状态（第一次访问的时间）
                            String firstVisitDate = firstVisitDateState.value();
                            //获取当前访问时间
                            Long ts = value.getLong("ts");
                            String curDate = sdf.format(ts);

                            if (firstVisitDate != null && firstVisitDate.length() != 0) {
                                //说明该设备已经访问过
                                if (!firstVisitDate.equals(curDate)) {
                                    isNew = "0";
                                    value.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
                                //说明该设备第一次访问
                                firstVisitDateState.update(curDate);
                            }
                        }
                        return value;
                    }
                });

        //5、侧输出流完成分流操作
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> splitDS = jsonObjWithFlagDS
                .process(new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                        //获取启动日志的属性
                        JSONObject startJsonObj = value.getJSONObject("start");
                        //为了向kafka写数据方便，将json对象转换的json字符串
                        String jsonStr = value.toJSONString();
                        if (startJsonObj != null && startJsonObj.size() > 0) {
                            //说明是启动日志
                            ctx.output(startTag, jsonStr);
                        } else {
                            //除启动日志外其他的都属于页面日志,写入主流
                            out.collect(jsonStr);
                            //判断是否为曝光日志
                            JSONArray displaysArr = value.getJSONArray("displays");
                            //获取页面id
                            String pageId = value.getJSONObject("page").getString("page_id");

                            if (displaysArr != null && displaysArr.size() > 0) {
                                //曝光日志写到侧输出流
                                for (int i = 0; i < displaysArr.size(); i++) {
                                    JSONObject displaysArrJSONObj = displaysArr.getJSONObject(i);
                                    displaysArrJSONObj.put("page_id", pageId);
                                    ctx.output(displayTag, displaysArrJSONObj.toJSONString());
                                }
                            }
                        }
                    }
                });

        //获取测输出流数据
        DataStream<String> startDS = splitDS.getSideOutput(startTag);
        DataStream<String> displayDS = splitDS.getSideOutput(displayTag);

        startDS.print("start");
        displayDS.print("display");
        splitDS.print("split");

        //6、将不同流的数据写回到kafka的dwd层
        String startTopic = "dwd_start_log";
        String displayTopic = "dwd_display_log";
        String pageTopic = "dwd_page_log";
        startDS.addSink(MyKafkaUtil.getKafkaSink(startTopic));
        displayDS.addSink(MyKafkaUtil.getKafkaSink(displayTopic));
        splitDS.addSink(MyKafkaUtil.getKafkaSink(pageTopic));

        env.execute();
    }
}

