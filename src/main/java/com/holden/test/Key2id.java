package com.holden.test;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-Key2id
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月20日21:27 - 周一
 * @Describe
 */
public class Key2id {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        DataStreamSource<String> one = env.socketTextStream("hadoop102", 31313);
        DataStreamSource<String> two = env.socketTextStream("hadoop102", 8888);


        ConnectedStreams<String, String> connect = one.connect(two);

        ConnectedStreams<String, String> c = connect.keyBy(x -> x.split(",")[0], y -> y.split(",")[0]);

        c.process(new CoProcessFunction<String, String, String>() {
            @Override
            public void processElement1(String value, CoProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect("one>>" + value);
            }

            @Override
            public void processElement2(String value, CoProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect("two>>" + value);

            }
        }).print();

        env.execute();
    }
}
