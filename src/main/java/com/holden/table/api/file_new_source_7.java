package com.holden.table.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

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
        //注册进环境
        tEnv.executeSql("create table sensor_source(" +
                "id STRING," +
                "`temp` DOUBLE," +
                "ts bigint," +
                "rt as TO_TIMESTAMP(FROM_UNIXTIME(ts, 'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for rt as rt - interval '5' second" +
                ") with (" +
                "'connector'='filesystem'," +
                "'format'='csv'," +
                "'csv.field-delimiter'=','," + //每个字段按,切分且这里不用指定每行数据的切分，默认一行一行读
                "'path'='T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt'" +
                ")");

        tEnv.sqlQuery("select * from sensor_source").execute().print();



        /*Table resTable = tEnv.from("sensor_source")
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));

        tEnv.toRetractStream(resTable, Row.class).print("cnt");*/

    }


}
