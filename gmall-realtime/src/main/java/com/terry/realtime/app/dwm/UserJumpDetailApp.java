package com.terry.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.terry.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000);
        //StateBackend fsStateBackend = new FsStateBackend("hdfs://hadoop102:9820/gmall/flink/checkpoint/UniqueVisitApp");
        //env.setStateBackend(fsStateBackend);
        //System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 1.从kafka中读取数据
        String groupId = "userJumpDetailApp";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwt_user_jump_detail";

        //TODO 2.读取kafka数据
        FlinkKafkaConsumer<String> source = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStream = env.addSource(source);

//        DataStream<String> jsonStream = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":15000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":30000} "
//                );

        //TODO 3.对读取的数据进行结构转换
        SingleOutputStreamOperator<JSONObject> jsonObjStream = jsonStream
                .map(jsonString -> JSON.parseObject(jsonString));

        //测试
        jsonObjStream.print("uv:");

        //TODO 4.分配水位线
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermark = jsonObjStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                return jsonObj.getLong("ts");
                            }
                        })
        );

        //TODO 5.使用flink-cep提取用户跳出明细
        //转化为键值流
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = jsonObjWithWatermark
                .keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //定义模式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        //lastpageid为空，视为跳出
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        if (lastPageId == null || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).next("second").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        if (pageId != null && pageId.length() > 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).within(Time.seconds(10));

        //将模式应用到流上
        PatternStream<JSONObject> patternDS = CEP.pattern(jsonObjectStringKeyedStream, pattern);

        //提取超时数据
        OutputTag<String> outputTag = new OutputTag<String>("side-output") {
        };

        SingleOutputStreamOperator<String> timeOutDS = patternDS.flatSelect(
                //默认情况下，每一个超时数据会自动打上该标签
                outputTag,
                //处理超时数据
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                        List<JSONObject> jsonObjectList = pattern.get("first");
                        for (JSONObject jsonObject : jsonObjectList) {
                            //向下游发送
                            out.collect(jsonObject.toJSONString());
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> pattern, Collector<String> out) throws Exception {
                        //非超时数据，属于正常跳转，不做处理
                    }
                }

        );

        //获取跳出流
        DataStream<String> jumpDS = timeOutDS.getSideOutput(outputTag);

        jumpDS.print("jumpDS");

        //将跳出明细写回kafka
        jumpDS.addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));

        env.execute();
    }
}
