package com.terry.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.terry.realtime.bean.OrderDetail;
import com.terry.realtime.bean.OrderInfo;
import com.terry.realtime.bean.OrderWide;
import com.terry.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.从kafka中读取订单数据
        String groupId = "orderwide_group";
        String orderInfoTopic = "dwd_order_info";
        //订单数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoTopic, groupId);
        DataStreamSource<String> orderInfoStrDS = env.addSource(orderInfoSource);
        //订单明细
        String orderDetailTopic = "dwd_order_detail";
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailTopic, groupId);
        DataStreamSource<String> orderDetailStrDS = env.addSource(orderDetailSource);

        //TODO 3.对数据进行转换
        //订单
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat simpleDateFormat = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        orderInfo.setCreate_ts(simpleDateFormat.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );

        //订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    SimpleDateFormat simpleDateFormat = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        orderDetail.setCreate_ts(simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                }
        );

        //orderInfoDS.print("orderInfoDS");
        //orderDetailDS.print("orderDetailDS");

        //TODO 4.指定Watermark以及提取事件时间对象
        //订单
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWatermark = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        })
        );

        //订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWatermark = orderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        })
        );

        //TODO 5.对两条流进行分组
        //订单
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithWatermark.keyBy(r -> r.getId());
        //订单明细
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithWatermark.keyBy(r -> r.getOrder_id());

        //TODO 6.双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });

        orderWideDS.print("orderWideDS");

        env.execute();
    }
}
