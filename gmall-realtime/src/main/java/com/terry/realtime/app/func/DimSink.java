package com.terry.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.terry.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //建立连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //获取目的地表名
        String tableName = jsonObj.getString("sink_table");

        //获取data数据
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            //生成执行语句
            String upsertSql = genUpsertSql(tableName, dataJsonObj);

            //创建数据库操作对象
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement(upsertSql);
                ps.execute();
                conn.commit();
                System.out.println("执行语句是：" + upsertSql);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("向Phoenix插入数据失败");
            } finally {
                if (ps != null) {
                    ps.close();
                }
            }
        }
    }

    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        String upsertSql = "upsert into " +
                GmallConfig.HBASE_SCHEMA + "." + tableName +
                " (" + StringUtils.join(dataJsonObj.keySet(), ",") +
                ") values('" + StringUtils.join(dataJsonObj.values(), "','") + "')";

        return upsertSql;
    }
}

