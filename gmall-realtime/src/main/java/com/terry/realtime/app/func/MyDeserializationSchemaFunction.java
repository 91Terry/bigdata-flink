package com.terry.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

public class MyDeserializationSchemaFunction implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        //定义JSON对象用于存放反序列化后的数据
        JSONObject result = new JSONObject();

        //获取库名和表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String table = split[2];

        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        //获取数据本身
        Struct struct = (Struct) sourceRecord.value();
        Struct after = struct.getStruct("after");
        JSONObject value = new JSONObject();

//        System.out.println("Value:" + value);
        if (after != null) {
            Schema schema = after.schema();
            for (Field field : schema.fields()) {
                String name = field.name();
                Object o = after.get(name);
//                System.out.println(name + ":" + o);
                value.put(name, o);
            }
        }

        //将数据放入JSON对象
        result.put("database", database);
        result.put("table", table);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)) {
            type = "insert";
        }
        result.put("type", type);
        result.put("data", value);

        //将数据传输出去
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}

