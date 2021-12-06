package com.atguigu.table.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-kafka_source_7
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日15:05 - 周一
 * @Describe 新版本的file_source方式
 */
public class file_new_source_7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //step-1 编写数据source的sql语句
        String sourceDDL =
                "create table sensor_source(id STRING," +
                        "`timeStamp` BIGINT," +
                        "temperature DOUBLE" +
                        ") with (" +
                        "'connector'='filesystem'," +
                        "'format'='csv'," +
                        "'csv.field-delimiter'=','," + //每个字段按,切分且这里不用指定每行数据的切分，默认一行一行读
                        "'path'='T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt'" +
                        ")";
        tEnv.executeSql(sourceDDL);

        /*//step-2 编写数据sink的sql语句
        String sinkDDL =
                "create table sensor_sink(id STRING," +
                        "`timeStamp` BIGINT," +
                        "temperature DOUBLE" +
                        ") with (" +
                        "'connector.type'='kafka'," +
                        "'update-mode'='append'," +
                        "'connector.version' = 'universal'," +
                        "'connector.topic'='flink'," + //每个字段按,切分
                        "'scan.startup.mode' = 'earliest-offset'," +
                        "'connector.properties.bootstrap.servers'='hadoop102:9092'," +
                        "'connector.startup-mode' = 'latest-offset'," +
                        "'format' = 'csv'" +
                        ")";*/


        Table resTable = tEnv.from("sensor_source")
                .groupBy($("id"))
                .select($("id"), $("timeStamp"), $("temperature"));
        tEnv.toRetractStream(resTable, Row.class).print("cnt");

        env.execute();
    }


}
