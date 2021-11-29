package com.atguigu.table;

import com.atguigu.source.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @ClassName FlinkDemo-Example_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月28日15:43 - 周日
 * @Describe
 */
public class Example_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //创建表的执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table table = tEnv.fromDataStream(dataStream);

        //todo 使用Table API
        Table resTable = table.select($("id"), $("temperature"))
                .where($("id").isEqual("sensor_3"));
        //Row的类型不要导错包了，执行Table API
        tEnv.toDataStream(resTable, Row.class).print("tableApI");


        //todo 使用Sql
        tEnv.createTemporaryView("sensor",table);//创建一张临时表用于读数据
        String sql = "select id,temperature from sensor where id='sensor_3'";
        Table sqlQuery = tEnv.sqlQuery(sql);
        tEnv.toDataStream(sqlQuery,Row.class).print("sql");

        //todo 上述两者的实现结果是一样的，哪种你熟悉就可以选择哪种
        env.execute();
    }
}
