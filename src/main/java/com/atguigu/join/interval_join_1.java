package com.atguigu.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

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

        SingleOutputStreamOperator<String> blue = env.socketTextStream("hadoop102", 31313)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return Long.parseLong(element.split(",")[2]);
                            }
                        }));

        SingleOutputStreamOperator<String> red = env.socketTextStream("hadoop102", 8888)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return Long.parseLong(element.split(",")[2]);
                            }
                        }));


        //Attention 必须指定为事件时间
        blue.keyBy(x -> x.split(",")[0])//指明蓝流中关联字段
                .intervalJoin(red.keyBy(y -> y.split(",")[0]))//指明红流中关联字段
                .between(Time.milliseconds(-10), Time.milliseconds(5))//lowerBound=red=保存10s,upperBound=blue=保存5s
                .process(new ProcessJoinFunction<String, String, String>() {
                    //ProcessJoinFunction<IN1,IN2,OUT>,其中IN1是orange流,IN2是green流
                    @Override
                    public void processElement(String left, String right, ProcessJoinFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("blue:" + left + " || red:" + right);
                    }
                }).print("join");
        /*
        * Example-1
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
        * Example-2
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
        * */

        env.execute();
    }
}
