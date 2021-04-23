package com.terry.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.terry.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint/UniqueVisitApp");
        //env.setStateBackend(fsStateBackend);
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 1、从kafka中读取数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwt_unique_visit";

        //读取kafka数据
        FlinkKafkaConsumer<String> source = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStream = env.addSource(source);

        //TODO 2.对读取的数据进行结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjStream = jsonStream
                .map(jsonString -> JSON.parseObject(jsonString));

        //测试
        //jsonObjStream.print("uv:");

        //TODO 3.按照mid对数据进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjStream
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 4.过滤非首日访问
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS
                .filter(
                        new RichFilterFunction<JSONObject>() {
                            private ValueState<String> lastVisitDateState;
                            private SimpleDateFormat sdf;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                sdf = new SimpleDateFormat("yyyy-MM-dd");

                                ValueStateDescriptor valueStateDescriptor =
                                        new ValueStateDescriptor("valueStateDescriptor", String.class);

                                //统计日活，设置状态存活时间
                                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();

                                valueStateDescriptor.enableTimeToLive(stateTtlConfig);

                                lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);

                            }

                            @Override
                            public boolean filter(JSONObject jsonObj) throws Exception {
                                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                                //如果最后一个页面有数据，说明是从别的页面跳转过来的，不是首次访问，过滤掉
                                if (lastPageId != null && lastPageId.length() > 0) {
                                    return false;
                                }

                                Long ts = jsonObj.getLong("ts");
                                String curDate = sdf.format(ts);
                                String lastViewDate = lastVisitDateState.value();

                                if (lastViewDate != null && lastViewDate.length() > 0 && curDate.equals(lastViewDate)) {
                                    return false;
                                } else {
                                    lastVisitDateState.update(curDate);
                                    return true;
                                }
                            }
                        }
                );
        //测试
        filterDS.print("filterDS");

        //TODO 6、将dwd过滤数据写入kafka的dwm层
        filterDS
                .map(jsonObj -> jsonObj.toJSONString())
                .addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));

        env.execute();
    }
}
