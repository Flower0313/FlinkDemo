package com.holden.sink;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @ClassName FlinkDemo-sink_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日18:42 - 周日
 * @Describe
 */
public class sink_kafka_1 {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境 & 数据
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);

        //Step-2 配置kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");

        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>("flink", new SimpleStringSchema(), properties);

        //将sensor.txt的数据发往kafka
        dataStream.addSink(myProducer);

        env.execute();
    }
}
