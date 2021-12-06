package com.atguigu.table.sql;

import com.atguigu.source.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-sql_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月28日19:29 - 周日
 * @Describe
 */
public class sql_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //todo 开始
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //未注册的表
        Table table = tableEnv.fromDataStream(dataStream, $("id"), $("timeStamp"), $("temperature"));
        tableEnv.sqlQuery("select id,timeStamp,temperature from" + table + "where id ='sensor_3'");

        //注册的表(一次查询产生一张新表)
        tableEnv.createTemporaryView("Sensor",dataStream);
        Table table1 = tableEnv.sqlQuery("select id,timeStamp,temperature from Sensor where id ='sensor_3'");



        env.execute();
    }
}




















