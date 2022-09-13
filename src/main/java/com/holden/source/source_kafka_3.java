package com.holden.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ClassName FlinkDemo-source_kafka_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月20日19:50 - 周六
 * @Describe 从kafka中读取数据
 */
public class source_kafka_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "43.142.117.50:9092");
        properties.setProperty("group.id", "flink");//消费者组
        properties.setProperty("auto.offset.reset","latest");//earliest
        DataStream<String> topic = env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));

        //打印输出
        topic.print("kafka");
        //这样就可以使用kafka发送消息了,这边就能接收
        env.execute();

    }
}

