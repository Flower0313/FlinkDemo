package com.atguigu.table.api;

import com.atguigu.source.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.http.conn.scheme.Scheme;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-Example_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月28日18:56 - 周日
 * @Describe 简版table api & sql
 */
public class Example_2 {

    public static void main(String[] args) throws Exception {
        //流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //表的执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //todo 注册sensor临时表
        tEnv.createTemporaryView("sensor", dataStream);

        //todo Table API
        Table tableApi = tEnv.from("sensor")
                .select($("id"), $("temperature"))
                .where($("id")
                        .isEqual("sensor_3"));

        //todo Sql
        Table sqlQuery = tEnv.sqlQuery("select id,temperature from sensor where id='sensor_3'");



        //todo 输出
        tEnv.toDataStream(tableApi, Row.class).print("api");
        tEnv.toDataStream(sqlQuery, Row.class).print("sql");

        //解释
        //System.out.println(tableApi.explain());


        env.execute();
    }
}
