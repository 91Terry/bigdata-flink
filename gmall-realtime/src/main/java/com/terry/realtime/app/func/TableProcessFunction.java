package com.terry.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.terry.realtime.bean.TableProcess;
import com.terry.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

//分流处理函数
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection conn;
    private OutputTag<JSONObject> dimOutputTag;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> dimOutputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.dimOutputTag = dimOutputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //建立连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    //处理业务流中的数据
    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //从状态中获取配置信息
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //获取业务表名
        String tableName = jsonObj.getString("table");
        //获取操作类型
        String type = jsonObj.getString("type");

        //注意：如果使用的maxwell的bootstrap接受历史数据时，接受的类型时bootstrap-insert
        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObj.put("type", type);
        }

        //拼接状态中的key
        String key = tableName + ":" + type;

        System.out.println(key);

        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess != null) {
            //获取数据信息
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            //获取输出目的地的表名或者主题名
            String sinkType = tableProcess.getSinkTable();
            jsonObj.put("sink_table", sinkType);

            //根据配置表中的sink_column对数据进行过滤
            String sinkColumns = tableProcess.getSinkColumns();
            if (sinkColumns != null && sinkColumns.length() > 0) {
                filterColumn(dataJsonObj, sinkColumns);
            }

            //分流，判断事实表还是维度表
            if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)) {
                //维度表，放到侧输出流
                ctx.output(dimOutputTag, jsonObj);
            } else if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)) {
                //事实表 ,放主流
                out.collect(jsonObj);
            }
        } else {
            System.out.println("No this Key in TableProcess:" + key);
        }
    }

    //根据配置表中的sinkColumn配置，对json中的数据进行过滤
    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] filedArr = sinkColumns.split(",");
        List<String> filedList = Arrays.asList(filedArr);

        Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();
        entries.removeIf(ele -> !filedList.contains(ele.getKey()));
    }


    //处理广播流中的数据
    @Override
    public void processBroadcastElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {
        //将字符串转化为json对象
        JSONObject jsonObj = JSON.parseObject(jsonStr);
        String dataJsonStr = jsonObj.getString("data");
        //将data字符串转化为tableprocess实体对象
        TableProcess tableProcess = JSON.parseObject(dataJsonStr, TableProcess.class);

        //获取源表名
        String sourceTable = tableProcess.getSourceTable();
        //获取操作类型
        String operateType = tableProcess.getOperateType();
        //获取输出类型
        String sinkType = tableProcess.getSinkType();
        //获取输出目的地表名或者主题名
        String sinkTable = tableProcess.getSinkTable();
        //输出字段
        String sinkColumns = tableProcess.getSinkColumns();
        //获取表的主键
        String sinkPk = tableProcess.getSinkPk();
        //建表扩展语句
        String sinkExtend = tableProcess.getSinkExtend();
        //拼接保存的key
        String key = sourceTable + ":" + operateType;

        //如果是维度数据，通过phoenix创建表
        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
        }

        //获取状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //将数据写入状态进行广播
        broadcastState.put(key, tableProcess);
    }

    private void checkTable(String tableName, String fields, String pk, String ext) {
        if (pk == null) {
            pk = "id";
        }

        if (ext == null) {
            ext = "";
        }

        String[] fieldArr = fields.split(",");

        //拼接建表语句
        //选用StringBuilder可变字符串
        StringBuilder createSql = new StringBuilder("create table if not exists " +
                GmallConfig.HBASE_SCHEMA + "." +
                tableName + "(");
        //遍历属性字段拼接建表语句
        for (int i = 0; i < fieldArr.length; i++) {
            String field = fieldArr[i];
            //判断当前字段是否为主键字段
            if (pk.equals(field)) {
                createSql.append(field).append(" varchar primary key");
            } else {
                createSql.append(field).append(" varchar ");
            }
            //除了最后一个字段都需要加逗号
            if (i < fieldArr.length - 1) {
                createSql.append(",");
            }
        }
        createSql.append(")" + ext);
        System.out.println("建表语句为：" + createSql);

        PreparedStatement ps = null;
        try {
            //创建操作对象
            ps = conn.prepareStatement(createSql.toString());
            //执行sql语句
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("在Phoenix中建表失败");
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

