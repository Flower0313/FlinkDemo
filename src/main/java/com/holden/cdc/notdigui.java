package com.holden.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName FlinkDemo-notdigui
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年7月22日10:42 - 周五
 * @Describe
 */
public class notdigui {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        tEnv.executeSql("create table orders(" +
                "`order_id` STRING," +
                "`price` decimal(10,2)," +
                "`currency` STRING," +
                "`timez` bigint," +
                "rt as TO_TIMESTAMP_LTZ(timez,3)," +
                "WATERMARK FOR rt AS rt," +
                "PRIMARY KEY (order_id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='127.0.0.1'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='root'," +
                "'database-name'='spider_base'," +
                "'table-name'='orders'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");

        //会join维表最大时间戳之前的数据。
        /*
         * 当orders的时间走到10秒时，此时currencys的时间还是1秒，那么join结果是出不来的，因为系统不知道10秒的时候维度变化了吗？
         * 所以只有当currencys对应的维度走到了10秒或以上，数据才会出来。
         * Processing-time temporal join目前不支持
         * 我的想法是在维度表中插入一个标记值，这个标记值的时间每天更新到最大
         * 若没有事件时间字段的话，每次将获取到的时间戳加24小时
         * */
        tEnv.executeSql("create table currencys(" +
                "`currency` string," +
                "`rate` decimal," +
                "`timez` bigint," +
                "rt as TO_TIMESTAMP_LTZ(timez,3)," +
                "WATERMARK FOR rt AS rt," +
                "PRIMARY KEY (currency) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='127.0.0.1'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='root'," +
                "'database-name'='spider_base'," +
                "'table-name'='currencys'" +
                ")");


        tEnv.sqlQuery("select orders.order_id,orders.currency,orders.price,orders.rt,currencys.rate,currencys.rt " +
                "from orders left join currencys FOR SYSTEM_TIME AS OF PROCTIME() " +
                "on orders.currency=currencys.currency").execute().print();
    }
}
