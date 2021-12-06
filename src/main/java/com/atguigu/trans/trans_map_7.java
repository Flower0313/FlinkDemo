package com.atguigu.trans;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-trans_map_6
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月26日17:50 - 周五
 * @Describe 消费一个元素产生一个元素
 */
public class trans_map_7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        dataStream.print("map");//默认rebalance轮询分区

        //dataStream.keyBy(SensorReading::getId).print("keyBy");//按hash分区

        env.execute();
    }

    public static class MyRichFunction extends RichMapFunction<Integer,Integer> {

        public void open(){
            System.out.println("open...执行一次");
        }

        @Override
        public Integer map(Integer integer) throws Exception {
            return null;
        }

        public void close(){
            System.out.println("close...执行一次");
        }
    }
}













