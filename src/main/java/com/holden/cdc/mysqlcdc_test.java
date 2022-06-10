package com.holden.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName FlinkDemo-mysqlcdc_test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月08日15:39 - 周三
 * @Describe Joins => https://nightlies.apache.org/flink/flink-docs-release-1.13/zh/docs/dev/table/sql/queries/joins/
 */
public class mysqlcdc_test {
    public static void main(String[] args) throws Exception {
        //注册流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //注册表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //Long值使用BIGINT来替代
        tEnv.executeSql("create table department(" +
                "id BIGINT," +
                "name String," +
                "create_time TIMESTAMP(3)," +
                "WATERMARK FOR `create_time` AS create_time - INTERVAL '10' SECONDS," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='127.0.0.1'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='root'," +
                "'database-name'='spider_base'," +
                "'table-name'='department'" +
                ")");

        tEnv.executeSql("create table employee(" +
                "`id` BIGINT," +
                "`name` String," +
                "`score` DOUBLE," +
                "`age` BIGINT," +
                "`dept_id` BIGINT," +
                "`create_time` TIMESTAMP(3)," +
                "WATERMARK FOR `create_time` AS `create_time` - INTERVAL '0' SECOND," +
                "`ts` as PROCTIME()," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='127.0.0.1'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='root'," +
                "'database-name'='spider_base'," +
                "'table-name'='employee'" +
                ")");

        Table table = tEnv.sqlQuery("select *,row_number() over(order by ts) as rk from employee");
        tEnv.toChangelogStream(table).print();






        /*
         * Attention 总结
         *  Interval Joins仅支持Append Only流
         *  只有在Top-N的情况下over()中的order by可以按照任意字段排序
         *  普通情况下，order by只能根据时间属性排序
         *  1.13后窗口本身就是一个表，所以是出现在from后面的
         *  当时间是yyyy-MM-dd的话直接使用TIMESTAMP，当时间为时间戳时使用TO_TIMESTAMP_LTZ(时间,3)
         *
         * */

    }
}
