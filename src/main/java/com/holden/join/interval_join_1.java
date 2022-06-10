package com.holden.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName FlinkDemo-interval_join_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月17日11:50 - 周五
 * @Describe
 */
public class interval_join_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> paymentInfo = env.socketTextStream("hadoop102", 31313)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return Long.parseLong(element.split(",")[2]) * 1000L;
                            }
                        }));

        SingleOutputStreamOperator<String> orderWide = env.socketTextStream("hadoop102", 8888)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return Long.parseLong(element.split(",")[2]) * 1000L;
                            }
                        }));


        //Attention 必须指定为事件时间
        paymentInfo.keyBy(x -> x.split(",")[0])//指明蓝流中关联字段
                .intervalJoin(orderWide.keyBy(y -> y.split(",")[0]))//指明红流中关联字段
                .between(Time.seconds(-10), Time.seconds(5))//lowerBound=red=保存10s,upperBound=blue=保存5s,默认[a,b]
                //.lowerBoundExclusive() //加了变成(a,b]
                //.upperBoundExclusive() //加了变成[a,b),都加了变成(a,b)
                .process(new ProcessJoinFunction<String, String, String>() {
                    //ProcessJoinFunction<IN1,IN2,OUT>,其中IN1是orange流,IN2是green流
                    @Override
                    public void processElement(String left, String right, ProcessJoinFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("paymentInfo:" + left + " || orderWide:" + right);
                        System.out.println("[" + ctx.getLeftTimestamp() + "," + ctx.getRightTimestamp() + "]");
                    }
                }).print("join");
        /*
         * Example-1-[-10,5]
         *  blue.join(red)
         * blue:保存5秒
         * s1,1,1 -- 保存至ts=6
         * s1,2,2
         * s1,3,3
         * s1,4,4
         * s1,5,5 -- 保存至ts=10,watermark=9999
         *
         * red:保存10秒
         * s1,6,6
         * s1,7,7
         * s1,10,10
         * s1,11,11
         *
         * Result-1
         * join> blue:s1,1,1 || red:s1,6,6
         * join> blue:s1,2,2 || red:s1,6,6
         * join> blue:s1,3,3 || red:s1,6,6
         * join> blue:s1,4,4 || red:s1,6,6
         * join> blue:s1,5,5 || red:s1,6,6 -- join还保存在状态中的数据
         * join> blue:s1,2,2 || red:s1,7,7
         * join> blue:s1,3,3 || red:s1,7,7
         * join> blue:s1,4,4 || red:s1,7,7
         * join> blue:s1,5,5 || red:s1,7,7
         * join> blue:s1,5,5 || red:s1,10,10 -- 时间走到了10秒(watermark=9999),正好join上保存至同一时刻的s1,5,5
         *
         *
         *
         * Example-2-[-10,5]
         * red:保存10秒
         * s1,1,1
         * s1,2,2
         * s1,3,3
         * s1,4,4
         * s1,5,5
         * s1,6,6
         * s1,7,7
         * s1,8,8
         * s1,9,9
         * s1,10,10 -- 保存至20秒,watermark=19999
         *
         * blue:保存5秒
         * s1,11,11
         * s1,20,20
         * s1,21,21
         *
         * Result-2
         * join> blue:s1,11,11 || red:s1,1,1
         * join> blue:s1,11,11 || red:s1,2,2
         * join> blue:s1,11,11 || red:s1,3,3
         * join> blue:s1,11,11 || red:s1,4,4
         * join> blue:s1,11,11 || red:s1,5,5
         * join> blue:s1,11,11 || red:s1,6,6
         * join> blue:s1,11,11 || red:s1,7,7
         * join> blue:s1,11,11 || red:s1,8,8
         * join> blue:s1,11,11 || red:s1,9,9
         * join> blue:s1,11,11 || red:s1,10,10
         * join> blue:s1,20,20 || red:s1,10,10 -- join上还保留着的数据也就是red:s1,10,10,因为它正好保存到20秒
         *
         *
         * Example-3-[-5,5]
         * blue:
         * s1,1,1
         * s2,12,10
         *
         * red:
         * s1,2,2
         * s1,3,3
         * s2,13,10
         * s1,4,4 -- 按道理的话这条应该能和s1,1,1关联上,但s2,12,10和s2,13,10已经将水位线推到了10,但因为s1,1,1只保留5秒所
         * 以这条数据已经过期了,当然关联不上,用公共watermark来管理数据的过期时间
         *
         * Result-3
         * join> blue:s1,1,1 || red:s1,2,2
         * join> blue:s1,1,1 || red:s1,3,3
         * join> blue:s2,12,10 || red:s2,13,10
         *
         * Conclusion
         * 1.这里是watermark取两条流中最小的,且不同的keyBy的id也共享一个水位线,且水位线是取两个流中最小的那个
         * 2.因为这里是两个source,所以下游join的两条流中watermark取最小的
         * */

        env.execute();
    }
}
