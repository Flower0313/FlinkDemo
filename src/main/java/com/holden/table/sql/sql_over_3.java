package com.holden.table.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName FlinkDemo-sql_over_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月20日9:57 - 周一
 * @Describe
 */
public class sql_over_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // step-1. 创建表的执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
/*
        "ts BIGINT," + //必须要先声明,rt才能使用,不能直接在rt那行使用ts
                "rt as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for rt as rt - interval '2' second" +*/
        String sourceDDL =
                "create table sensor(id STRING," +
                        "`timeStamp` BIGINT," +
                        "temperature DOUBLE," +
                        "`ts` AS TO_TIMESTAMP_LTZ(`timeStamp`, 3)," +
                        "WATERMARK FOR `ts` AS `ts` - INTERVAL '1' SECOND," +
                        "`rt` as proctime()" +
                        ") with (" +
                        "'connector'='filesystem'," +
                        "'format'='csv'," +
                        "'path'='T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt'" +
                        ")";
        tEnv.executeSql(sourceDDL);
        /*
        * 时间字段必须要加水位线才行
        * */
        tEnv.sqlQuery(
                        "select *,row_number() over(order by `rt`) from sensor"
                )
                .execute().print();

    }//,row_number() over(order by rt) as rk
}
