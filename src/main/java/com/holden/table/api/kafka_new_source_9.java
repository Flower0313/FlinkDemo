package com.holden.table.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName FlinkDemo-kafka_new_source_9
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日17:45 - 周一
 * @Describe
 */

@AllArgsConstructor
@NoArgsConstructor
@Data
class TimeTest {
    String id;
    String temp;
    String ts;
}

public class kafka_new_source_9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //这里从kafka传过去的必须严格为这种格式
        /*
         * 格式1:{"id":"s1","timeStamp":"313","temperature":"31"}
         * 格式2:{"id":"s2","timeStamp":"313","temperature":"31","ts":"22"}
         * 格式3:{"id":"s1","timeStamp":"313","ts":"31"}
         * 格式4:{"id":"s1"}
         * 可以得出结论,group by 字段是必须要的,其他多了的字段也不会映射上,根据kafka中key名称和字段名字来映射,
         * 相同的话就可以匹配上
         * TO_TIMESTAMP(FROM_UNIXTIME(ts, 'yyyy-MM-dd HH:mm:ss'))将时间戳转换成指定格式
         * 请注意TIMESTAMP(3)只能接受2017-10-10 10:01:01这样的时间格式
         * 而你若想填写数字类型的时间戳只能声明为as TO_TIMESTAMP(FROM_UNIXTIME(ts, 'yyyy-MM-dd HH:mm:ss'))
         *
         *
         * FROM_UNIXTIME(44)函数将返回1970-01-01 00:00:44
         * */
        tEnv.executeSql(
                "create table kafka_source(" +
                        "id STRING," +
                        "`temp` DOUBLE," +
                        "ts timeStamp(3)," +
                        "watermark for ts as ts - interval '2' second" + //这里只是声明延迟时间
                        ") with (" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'sensor'," +
                        "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                        "'properties.group.id' = 'bigdata'," +
                        "'format' = 'json'," + //接收到的kafka都是json格式或csv
                        "'json.ignore-parse-errors' = 'true'" +
                        ")");

        //窗口大小为10,延迟时间为2秒,窗口必须要写在group by中
        Table table = tEnv.sqlQuery("select " +
                "id,temp,ts from kafka_source " +
                "group by tumble(ts,interval '10' second),id,temp,ts");


        tEnv.toAppendStream(table, TimeTest.class).print();
        env.execute();

    }
}

