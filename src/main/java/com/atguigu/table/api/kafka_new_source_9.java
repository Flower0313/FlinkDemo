package com.atguigu.table.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-kafka_new_source_9
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日17:45 - 周一
 * @Describe
 */
public class kafka_new_source_9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "create table kafka_source(" +
                        "`id` STRING," +
                        "`timestamp` BIGINT," +
                        "`temp` DOUBLE" +
                        ")with(" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'sensor'," +
                        "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                        "'properties.group.id' = 'bigdata'," +
                        "'format' = 'json'," + //接收到的kafka都是json格式或csv
                        "'json.ignore-parse-errors' = 'true'" +
                        ")");

        Table table = tEnv.from("kafka_source")
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        tEnv.toRetractStream(table, Row.class).print();

        env.execute();
    }
}
