package com.holden.table.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName FlinkDemo-kafkatest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月07日14:37 - 周二
 * @Describe
 */
public class kafkatest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //{"id":"s1"}
        tEnv.executeSql(
                "create table kafka_source(" +
                        "id STRING" +
                        ") with (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'sensor'," +
                        "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                        "'properties.group.id' = 'bigdata'," +
                        "'format' = 'json'," + //接收到的kafka都是json格式或csv
                        "'json.ignore-parse-errors' = 'true'" +
                        ")");

        //窗口大小为10,延迟时间为2秒,窗口必须要写在group by中
        Table table = tEnv.sqlQuery("select * from kafka_source");


        tEnv.toAppendStream(table, Row.class).print();
        env.execute();
    }
}
