package com.atguigu.practice.cep;

import com.atguigu.bean.LoginEvent;
import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.atguigu.common.CommonEnv.LOGIN_SOURCE;
import static com.atguigu.common.CommonEnv.ORDER_SOURCE;

/**
 * @ClassName FlinkDemo-flink_orderWatch_8
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月11日11:58 - 周六
 * @Describe 订单支付实时监控
 */
public class flink_orderWatch_8 {
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
                            Long.parseLong(e[3]) * 1000L);
                })//注册水位线时间戳
                .assignTimestampsAndWatermarks(
                        //水位线延迟20秒,容错可以有20秒的数据
                        WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner((e, r) -> e.getEventTime())
                ).keyBy(OrderEvent::getOrderId);

        //Step-3 准备模式
        Pattern<OrderEvent, OrderEvent> orderEventPattern = Pattern
                .<OrderEvent>begin("create")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        /*
         * Explain 定义在15分钟之内完成创建订单并支付订单的数据集
         * */

        //Step-4 注册模式
        PatternStream<OrderEvent> pattern = CEP.pattern(dataSource, orderEventPattern);

        //Step-5 输出数据
        SingleOutputStreamOperator<String> result = pattern
                .select(new OutputTag<String>("timeout") {},
                        new PatternTimeoutFunction<OrderEvent, String>() {
                            @Override
                            public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                                return pattern.toString();
                            }
                        },
                        new PatternSelectFunction<OrderEvent, String>() {
                            @Override
                            public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                                return pattern.toString();
                            }
                        });
        //Step-6 输出侧输出流的超时数据
        result.getSideOutput(new OutputTag<String>("timeout") {}).print("超时数据");

        //Step-7 输出满足条件的数据
        result.print("正常数据");

        env.execute();
    }
}
