package com.holden.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName FlinkDemo-mysqlcdc
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月08日15:30 - 周三
 * @Describe
 */
public class mysqlcdc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //Long值使用BIGINT来替代
        //没有PROCTIME()属性字段的表都是维表，一般维表的是变化不频繁的
        //写出外部数据库也要指明唯一的key，能根据key值进行更改的，比如es和redis就有幂等性
        tEnv.executeSql("create table by_contract(" +
                "id BIGINT," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='business_uat_2'," +
                "'table-name'='by_contract'" +
                ")");

        Table table = tEnv.sqlQuery("select * from by_contract");
        tEnv.toAppendStream(table,Row.class).print();
//        table.execute().print();
        env.execute();
    }
}
