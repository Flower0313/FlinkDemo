package com.atguigu.process;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-process_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月05日10:58 - 周日
 * @Describe
 */
public class process_3 {
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//并行度1，只开放一个slot


        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        /*
         * Explain
         * 这个证明process可以作用于DataStream也可以作用于KeyedStream
         * 泛型参数1是input的类型
         * 泛型参数2是output的类型
         * */
        dataStream.process(new ProcessFunction<SensorReading, Object>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading,
                    Object>.Context ctx, Collector<Object> out) throws Exception {
                out.collect(new Tuple2<>(value.getId(), value.getTemperature()));

            }
        }).print("before-keyBy");

        dataStream.keyBy(SensorReading::getId).process(new ProcessFunction<SensorReading, Object>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, Object>.Context ctx, Collector<Object> out) throws Exception {
                out.collect(new Tuple2<>(value.getId(), value.getTemperature()));
            }
        }).print("after-keyBy");


        env.execute();
    }
}
