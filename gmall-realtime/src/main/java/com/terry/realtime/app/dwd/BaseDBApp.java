package com.terry.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.terry.realtime.app.func.DimSink;
import com.terry.realtime.app.func.MyDeserializationSchemaFunction;
import com.terry.realtime.app.func.TableProcessFunction;
import com.terry.realtime.bean.TableProcess;
import com.terry.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //1、基本环境准备
        //创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);

//        //检查点相关设置
//        //开启检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        //设置状态后端(内存/文件系统/RockDB)
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9820/gmall/db_checkpoint"));
//        //设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
//        //设置操作HDFS的用户
//        System.setProperty("HADOOP_USER_NAME","atguigu");

        //2、从kafka主题中读数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));

        //3、对流中数据进行结构的转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value);
                    }
                });

        //4、对转换之后的数据进行简单的ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {

                        boolean flag = value.getString("table") != null
                                && value.getString("table").length() > 0
                                && value.getJSONObject("data") != null
                                && value.getString("data").length() > 3;
                        return flag;
                    }
                });

        filterDS.print("filterDS");

        //5、通过Flink-CDC读取mysql配置表
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("admin")
                .databaseList("gmall2021_realtime")
                .tableList("gmall2021_realtime.table_process")
                .deserializer(new MyDeserializationSchemaFunction())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mySQLDS = env.addSource(sourceFunction);
        mySQLDS.print("mySQLDS");

        //TODO 6.将配置数据形成广播流向下广播
        //定义状态描述器
        MapStateDescriptor<String, TableProcess> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcess>("table-process", String.class, TableProcess.class);
        //获取广播流
        BroadcastStream<String> broadcastStream = mySQLDS.broadcast(mapStateDescriptor);

        //将主流和广播流数据连接到一起
        BroadcastConnectedStream<JSONObject, String> connectStream = filterDS.connect(broadcastStream);

        //对连接之后的流进行处理--分流  维度表——测输出流  事实表——主流
        OutputTag<JSONObject> dimOutputTag = new OutputTag<JSONObject>("dimTag") {
        };

        SingleOutputStreamOperator<JSONObject> splitDS = connectStream
                .process(new TableProcessFunction(dimOutputTag, mapStateDescriptor));

        //获取测输出流
        DataStream<JSONObject> dimDS = splitDS.getSideOutput(dimOutputTag);

        splitDS.print("splitDS");
        dimDS.print("dimDS");

        //TODO 7、将维度侧输出流的数据插入到Phoenix
        dimDS.addSink(new DimSink());

        //TODO 8、将事实数据写入kafka
        splitDS.addSink(MyKafkaUtil.getKafkaBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                //数据保存到kafka哪个主题
                String topic_name = jsonObj.getString("sink_table");
                //获取data数据
                JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                return new ProducerRecord<>(topic_name, dataJsonObj.toString().getBytes());
            }
        }));

        env.execute();
    }
}
