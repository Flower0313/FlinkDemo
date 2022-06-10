package com.holden.table.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName FlinkDemo-jdbc_source
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月07日13:55 - 周二
 * @Describe
 */
public class jdbc_source {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //step-1 编写数据source的sql语句
        //注册进环境
        tEnv.executeSql("create table employee(" +
                "number Integer," +
                "diff DOUBLE," +
                "score DOUBLE," +
                "PRIMARY KEY (number) NOT ENFORCED" +
                ") with (" +
                "'connector'='jdbc'," +
                "'url'='jdbc:mysql://localhost:3306/spider_base'," +
                "'table-name'='employee'," +
                "'username'='root'," +
                "'password'='root'" +
                ")");

        Table table = tEnv.sqlQuery("select * from employee");
        tEnv.toAppendStream(table, Row.class).print();
        env.execute();
    }

}
