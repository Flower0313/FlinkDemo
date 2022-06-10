package com.holden.table.api;

import com.holden.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-file_new_sink_11
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日18:17 - 周一
 * @Describe
 */
public class file_new_sink_11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //step-1 建造数据源
        DataStreamSource<SensorReading> waterSensorStream =
                env.fromElements(new SensorReading("sensor_1", 1000L, 10D),
                        new SensorReading("sensor_1", 2000L, 20D),
                        new SensorReading("sensor_2", 3000L, 30D),
                        new SensorReading("sensor_1", 4000L, 40D),
                        new SensorReading("sensor_1", 5000L, 50D),
                        new SensorReading("sensor_2", 6000L, 60D));

        //step-2 处理数据
        Table resTable = tEnv.fromDataStream(waterSensorStream)
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("timeStamp"), $("temperature"));


        tEnv.executeSql(
                "create table file_sink(" +
                        "`id` STRING," +
                        "`timestamp` BIGINT," +
                        "`temp` DOUBLE" +
                        ")with(" +
                        "'connector' = 'filesystem'," +
                        "'format'='csv'," + //可以填写json
                        //explain 当解析异常时，是跳过当前字段或行，还是抛出错误失败（默认为 false，即抛出错误失败）。如果忽略字段的解析异常，则会将该字段值设置为null。
                        "'csv.ignore-parse-errors' = 'true'," +
                        //explain 是否允许忽略注释行（默认不允许），注释行以 '#' 作为起始字符。 如果允许注释行，请确保 csv.ignore-parse-errors 也开启了从而允许空行。
                        "'csv.allow-comments' = 'true'," +
                        "'path'='output/sink_new.txt'" +
                        ")");

        resTable.executeInsert("file_sink");
    }
}
