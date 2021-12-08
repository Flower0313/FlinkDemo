package com.atguigu.practice.app;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

import static com.atguigu.common.CommonEnv.ORDER_SOURCE;
import static com.atguigu.common.CommonEnv.RECEIPT_SOURCE;

/**
 * @ClassName FlinkDemo-flink_OrderPay_6
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日18:13 - 周三
 * @Describe 订单支付实时监控
 */
public class flink_OrderPay_6 {
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

        //step-3 将两个流连接在一起
        ConnectedStreams<OrderEvent, TxEvent> orderAndTs = orderEventDS.connect(txDS);

        //step-4 因为不同的数据流到达的先后顺序不同,所以需要匹配对账信息,输出表示对账成功与否
        /*
         * Explain
         * 1.这里两个流中都有txId字段,所以通过这个字段可以进行关联分组
         * 2.两条流中同txId的数据会分到同一个分区
         * 3.不会产生笛卡尔积
         * */
        orderAndTs
                .keyBy("txId", "txId")
                .process(new OTProcessFunction()).print();

        env.execute();
    }

    public static class OTProcessFunction extends CoProcessFunction<OrderEvent, TxEvent, Object> {
        //sub-step 定义 txId -> OrderEvent,存储的都是未完成支付的订单
        Map<String, OrderEvent> orderMap = new HashMap<>();

        //sub-step 定义 txId -> TxEvent
        Map<String, TxEvent> txMap = new HashMap<>();

        /**
         * 处理Order流,因为是Order流调用的connect,可以通过查看CoProcessFunction内部泛型对应的位置来判断
         *
         * @param value Order流中的每条数据
         * @param ctx   上下文
         * @param out   输出属性
         */
        @Override
        public void processElement1(OrderEvent value, CoProcessFunction<OrderEvent, TxEvent, Object>.Context ctx, Collector<Object> out) throws Exception {
            //查看本条订单中的交易码是否在账单数据中,若在则表明此订单已付款
            if (txMap.containsKey(value.getTxId())) {
                out.collect("订单:" + value + "对账成功");
                //完成订单的交易码就在支付表中删除
                txMap.remove(value.getTxId());
            } else {
                //若订单数据没有支付就不会在交易表中出现,此时就把数据加入到orderMap
                orderMap.put(value.getTxId(), value);
            }
        }

        /**
         * 处理Receipt流,因为它处于connect()的方法中
         *
         * @param value Receipt流中的每条数据
         */
        @Override
        public void processElement2(TxEvent value, CoProcessFunction<OrderEvent, TxEvent, Object>.Context ctx, Collector<Object> out) throws Exception {
            //若此条支付事件数据的txId在未支付的订单表中出现,则表明对应的订单表已经完成支付了
            if (orderMap.containsKey(value.getTxId())) {
                //取出对应上的订单数据
                out.collect("订单:" + orderMap.get(value.getTxId()) + "对账成功");
            } else {
                /*
                 * Explain
                 * 将未匹配上的订单事件加入到Map集合中
                 *
                 * Q&A！
                 * Q1:按道理来说,有订单支付信息表示订单表一定存在,为什么还能没对应上呢?
                 * A1:由于网络传输延迟的原因,用户可能可能创建订单就支付了,但是支付数据先到,然后订单数据
                 * 再过来,这就会导致进来的支付信息对应不上的情况
                 * */
                txMap.put(value.getTxId(), value);
            }
        }
    }
}
