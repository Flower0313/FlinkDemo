package com.holden.state;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-BroadcastState_11
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日19:14 - 周二
 * @Describe
 */
public class BroadcastState_11 {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //313流
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 31313);

        //88流
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop102", 8888);

       dataStream.connect(controlStream)
                       .process(new CoProcessFunction<String, String, Object>() {
                           @Override
                           public void processElement1(String value, CoProcessFunction<String, String, Object>.Context ctx, Collector<Object> out) throws Exception {

                           }

                           @Override
                           public void processElement2(String value, CoProcessFunction<String, String, Object>.Context ctx, Collector<Object> out) throws Exception {

                           }
                       });


        env.execute();

    }
}
