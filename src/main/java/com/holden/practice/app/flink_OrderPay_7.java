package com.holden.practice.app;

import com.holden.bean.OrderEvent;
import com.holden.bean.TxEvent;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import static com.holden.common.CommonEnv.ORDER_SOURCE;
import static com.holden.common.CommonEnv.RECEIPT_SOURCE;

/**
 * @ClassName FlinkDemo-flink_OrderPay_6
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日18:13 - 周三
 * @Describe 订单支付实时监控
 */
public class flink_OrderPay_7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //step-1 读取Order流
        DataStream<OrderEvent> orderEventDS = env.readTextFile(ORDER_SOURCE)
                .map(line -> {
                    String[] data = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.valueOf(data[3])
                    );
                });

        //step-2 读取Receipt流
        DataStream<TxEvent> txDS = env.readTextFile(RECEIPT_SOURCE)
                .map(line -> {
                    String[] data = line.split(",");
                    return new TxEvent(
                            data[0],
                            data[1],
                            Long.valueOf(data[2])
                    );
                });

        /*
         * Explain:
         * 数据会这样过来:
         * 1,null
         * 2,2
         * 3,3
         * null,4
         * 5,5
         * 6,null
         * null,7
         * */
        orderEventDS.coGroup(txDS).where(OrderEvent::getTxId).equalTo(TxEvent::getTxId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<OrderEvent, TxEvent, Object>() {
                    @Override
                    public void coGroup(Iterable<OrderEvent> first, Iterable<TxEvent> second, Collector<Object> out) throws Exception {
                        out.collect("full outer join: " + first + " => " + second);
                    }
                }).print("full join");

        orderEventDS.coGroup(txDS).where(OrderEvent::getTxId).equalTo(TxEvent::getTxId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<OrderEvent, TxEvent, Object>() {
                    @Override
                    public void coGroup(Iterable<OrderEvent> first, Iterable<TxEvent> second, Collector<Object> out) throws Exception {
                        if (first.iterator().hasNext() && second.iterator().hasNext()) {
                            out.collect("join: " + first + " => " + second);
                        }
                    }
                }).print("inner join");

        orderEventDS.coGroup(txDS).where(OrderEvent::getTxId).equalTo(TxEvent::getTxId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<OrderEvent, TxEvent, Object>() {
                    @Override
                    public void coGroup(Iterable<OrderEvent> first, Iterable<TxEvent> second, Collector<Object> out) throws Exception {
                        if (first.iterator().hasNext()) {
                            out.collect("left join: " + first + " => " + second);
                        }
                    }
                }).print("left join");

        orderEventDS.coGroup(txDS).where(OrderEvent::getTxId).equalTo(TxEvent::getTxId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<OrderEvent, TxEvent, Object>() {
                    @Override
                    public void coGroup(Iterable<OrderEvent> first, Iterable<TxEvent> second, Collector<Object> out) throws Exception {
                        if (second.iterator().hasNext()) {
                            out.collect("right join: " + first + " => " + second);
                        }
                    }
                }).print("right join");

        env.execute();
    }


}
