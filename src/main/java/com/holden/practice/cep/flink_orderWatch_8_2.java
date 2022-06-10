package com.holden.practice.cep;

import com.holden.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

import static com.holden.common.CommonEnv.ORDER_SOURCE;

/**
 * @ClassName FlinkDemo-flink_orderWatch_8_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月17日17:35 - 周五
 * @Describe
 */
public class flink_orderWatch_8_2 {
    public static void main(String[] args) throws Exception {
        //Step-1 注册环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        env.setParallelism(1);

        //Step-2 准备数据
        KeyedStream<OrderEvent, Long> dataSource = env.readTextFile(ORDER_SOURCE)
                .map(ele -> {
                    String[] e = ele.split(",");
                    return new OrderEvent(Long.valueOf(e[0]),
                            e[1],
                            e[2],
                            Long.parseLong(e[3]));
                })//注册水位线时间戳
                .assignTimestampsAndWatermarks(
                        //水位线延迟20秒,容错可以有20秒的数据
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner((e, r) -> e.getEventTime() * 1000L)
                ).keyBy(OrderEvent::getOrderId);

        //Step-3 定义超时标签
        OutputTag<String> timeOutTag = new OutputTag<String>("time") {
        };


        SingleOutputStreamOperator<String> process = dataSource.process(new OrderWatchFunction());
        process.print("正常数据>>");
        process.getSideOutput(timeOutTag).print("超时数据>>");
        env.execute();
    }

    public static class OrderWatchFunction extends KeyedProcessFunction<Long, OrderEvent, String> {
        private ValueState<Long> lastTimeState;
        private ValueState<Long> payFirstState;//若一个订单的信息pay比create先来,因为网络传输问题

        @Override
        public void open(Configuration parameters) throws Exception {
            lastTimeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("time", Long.class));
            payFirstState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pay", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, KeyedProcessFunction<Long, OrderEvent, String>.Context ctx, Collector<String> out) throws Exception {
            if ("create".equals(value.getEventType())) {
                if (payFirstState.value() != null) {
                    if (Math.abs(value.getEventTime() - payFirstState.value()) >= 900) {//15mins=15*60
                        ctx.output(new OutputTag<String>("time") {
                        }, ctx.getCurrentKey() + "," + value.getEventType());
                    } else {
                        out.collect(ctx.getCurrentKey() + "," + value.getEventType());
                    }
                }
                lastTimeState.update(value.getEventTime());
            } else if ("pay".equals(value.getEventType())) {
                //若上次时间为空,则证明pay先来,乱序数据
                if (lastTimeState.value() == null) {
                    payFirstState.update(value.getEventTime());
                } else if (Math.abs(value.getEventTime() - lastTimeState.value()) >= 900) {
                    System.out.println(ctx.getCurrentKey() + "," + (Math.abs(value.getEventTime() - lastTimeState.value())));
                    ctx.output(new OutputTag<String>("time") {
                    }, ctx.getCurrentKey() + "," + value.getEventType());
                } else {
                    out.collect(ctx.getCurrentKey() + "," + value.getEventType());
                }
            }
        }
    }
}
