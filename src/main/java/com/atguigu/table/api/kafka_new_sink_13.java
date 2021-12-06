package com.atguigu.table.api;

import com.atguigu.source.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-kafka_new_sink_13
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日18:46 - 周一
 * @Describe
 */
public class kafka_new_sink_13 {
    public static void main(String[] args) {
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
        Table sensorTable = tEnv.fromDataStream(waterSensorStream);
        Table resultTable = sensorTable
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("timeStamp"), $("temperature"));


        tEnv.executeSql(
                "create table kafka_sink(" +
                        "`id` STRING," +
                        "`timestamp` BIGINT," +
                        "`temp` DOUBLE" +
                        ")with(" +
                        "'connector' = 'kafka'," +
                        "'topic' = 'sensor'," +
                        "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                        "'properties.group.id' = 'bigdata'," +
                        "'format' = 'json'," +
                        "'json.ignore-parse-errors' = 'true'" +
                        ")");
        /*
        * Conclusion
        * 对于source和sink上面的配置文件是没有办法影响的,source和sink的配置文件都是一样的;
        * 影响source和sink的就是下面调用的executeInsert("kafka_sink")和
        * tEnv.from("kafka_source"),这才是影响source和sink的方法。
        * */

        resultTable.executeInsert("kafka_sink");
    }
}
