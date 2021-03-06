package com.holden.trans;

import com.holden.bean.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @ClassName FlinkDemo-trans_part_6
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日17:55 - 周日
 * @Describe 重分区操作
 */
public class trans_part_6 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //访问http://localhost:8081/#/overview
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //env.setParallelism(3);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0] + "_" + new Random().nextInt(3), new Long(fields[1]), new Double(fields[2]));
        });

        //dataStream.print("origin");

        //dataStream.print("input");


        //1.shuffle，随机分区，底层是random,
        //dataStream.shuffle().print("shuffle");


        //2.KeyBy按照hash分区，相同key一定在一个分区上
        dataStream.keyBy(SensorReading::getId).process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {

            }
        });

        //3.global 把所有数据放到第一个分区中
//        dataStream.global().print("global");

        //4.rebalance,底层是轮询策略，前后两个任务并行度不一样就默认用这个方式
        //dataStream.rebalance().print("rebalance");

        env.execute();
    }
}


