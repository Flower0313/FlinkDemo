package com.holden.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName FlinkDemo-mysqlcdc
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月08日15:21 - 周三
 * @Describe
 */
public class jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //Long值使用BIGINT来替代
        tEnv.executeSql("create table `result`(" +
                "name String," +
                "num Bigint," +
                "PRIMARY KEY (name) NOT ENFORCED" +
                ") with (" +
                "'connector'='jdbc'," +
                "'url'='jdbc:mysql://127.0.0.1:3306/spider_base?characterEncoding=UTF-8'," +
                "'table-name'='result'," +
                "'username'='root'," +
                "'password'='root'" +
                ")");

        tEnv.executeSql("insert into `result` values('你好',1007)");
    }
}
