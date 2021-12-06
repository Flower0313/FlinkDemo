package com.atguigu.table.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-kafka_source_8
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日17:30 - 周一
 * @Describe 接收来自kafka的数据，数据必须是json格式
 *              {"id":"sensor_1","timeStamp":"2","temperature":"2"}
 *              https://www.bilibili.com/video/BV1qy4y1q728?p=86&spm_id_from=pageDriver
 */
public class kafka_old_source_8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        //step-1 表的元数据
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("timeStamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.FLOAT());

        //接收来自sensor主题的消息
        tEnv.connect(new Kafka()
                        .version("universal") //kafka通用版本
                        .topic("sensor")
                        .startFromLatest()
                        .property("group.id", "bigdata")
                        .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092"))
                .withFormat(new Json())//kafka那边是json格式，这里也可以写 new Csv()
                .withSchema(schema)
                .createTemporaryTable("sensor_kafka");

        //step-2 正式读取数据,from就说明是source
        Table table = tEnv.from("sensor_kafka")
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        tEnv.toRetractStream(table, Row.class).print();
        env.execute();
    }
}
