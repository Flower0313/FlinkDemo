package com.holden.table.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName FlinkDemo-KafkaPipeline
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日22:53 - 周一
 * @Describe
 */
public class KafkaPipeline {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //step-1 注册source_sensor
        tEnv.executeSql(
                "create table source_sensor(" +
                        "`id` STRING," +
                        "`timestamp` BIGINT," +
                        "`temp` DOUBLE" +
                        ")with(" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'topic_source_sensor'," +
                        "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                        "'properties.group.id' = 'bigdata'," +
                        "'format' = 'csv'" + //接收到的kafka都是json格式或csv
                        ")");


        //step-2 注册sink_sensor
        tEnv.executeSql(
                "create table sink_sensor(" +
                        "`id` STRING," +
                        "`timestamp` BIGINT," +
                        "`temp` DOUBLE" +
                        ")with(" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'topic_sink_sensor'," +
                        "'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                        "'properties.group.id' = 'bigdata'," +
                        "'format' = 'csv'" +
                        ")");

        //step-3 从sourceTable查询数据，并写入到sinkTable
        tEnv.executeSql("insert into sink_sensor select * from source_sensor where id='sensor_1'");

    }
}
