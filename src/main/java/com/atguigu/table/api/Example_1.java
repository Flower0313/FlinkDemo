package com.atguigu.table.api;

import com.atguigu.bean.SensorReading;
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

        //TODO 1.Blink Streaming Query
        EnvironmentSettings bsSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode().build();

        //TODO 2.Blink Batch Query
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode().build();

        //TODO 3.Flink Batch Query
        EnvironmentSettings fSetting = EnvironmentSettings.newInstance().build();

        //TODO 创建表的执行环境,参数二若不传,默认调用第3个,Flink Batch Query
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //TODO table的数据来自于dataStream,将DataStream转换为Table
        Table table = tEnv.fromDataStream(dataStream);

        //step 使用Table API
        Table resTable = table.select($("id"), $("temperature"))
                .where($("id").isEqual("sensor_3"));

        Table sumTable = table.groupBy($("id"))
                .select($("id"), $("timeStamp").sum().as("tsSum"));
        //Row的类型不要导错包了，执行Table API
        //tEnv.toDataStream(resTable, Row.class).print("tableApI");

        //attention 用到了groupBy和sum的话就需要用RetractStream流
        tEnv.toRetractStream(sumTable, Row.class).print("sumApI");


        //step 使用SQL
        tEnv.createTemporaryView("sensor", table);//注册一张临时表用于读数据
        Table sqlQuery = tEnv.sqlQuery("select id,temperature from sensor where id='sensor_3'");

        //todo sql执行方式一
        tEnv.executeSql("select id,sum(temperature) t from sensor group by id").print();
        //todo sql执行方式二
        tEnv.toDataStream(sqlQuery, Row.class).print("sql");

        //todo 上述两者的实现结果是一样的，哪种你熟悉就可以选择哪种
        env.execute();
    }
}
