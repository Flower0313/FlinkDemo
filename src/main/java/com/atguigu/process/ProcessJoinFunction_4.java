package com.atguigu.process;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @ClassName FlinkDemo-ProcessJoinFunction_4
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月09日10:58 - 周四
 * @Describe Window Join流的测试
 */
public class ProcessJoinFunction_4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//并行度1，只开放一个slot

        //step-1 事件时间流s1
        DataStream<SensorReading> s1 = env.socketTextStream("hadoop102", 8888)
                .map(value -> {
                    String[] s = value.split(",");
                    return new SensorReading(s[0], Long.valueOf(s[1]), Double.valueOf(s[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                            @Override
                            public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                return element.getTimeStamp() * 1000L;
                            }
                        }));

        //step-2 事件时间流s2
        DataStream<SensorReading> s2 = env.socketTextStream("hadoop102", 31313)
                .map(value -> {
                    String[] s = value.split(",");
                    return new SensorReading(s[0], Long.valueOf(s[1]), Double.valueOf(s[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                                    @Override
                                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                        return element.getTimeStamp() * 1000L;
                                    }
                                }));

        /*
         * Explain
         * 1.同一个窗口相同的key会进行笛卡尔积关联,两条都必须在同一个窗口
         * */

        DataStream<String> stream = s1.join(s2)
                .where(SensorReading::getId)
                .equalTo(SensorReading::getId)
                //必须使用窗口,这就是Window Join
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<SensorReading, SensorReading, String>() {
                    @Override
                    public String join(SensorReading first, SensorReading second) throws Exception {
                        System.out.println("-- 调用 -- ");
                        return "s1:" + first + ",s2:" + second;
                    }
                });

        stream.print("JoinProcessFunction");
        env.execute();
    }
}
