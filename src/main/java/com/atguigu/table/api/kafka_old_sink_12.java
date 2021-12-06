package com.atguigu.table.api;

import com.atguigu.source.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-kafka_old_sink_12
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日18:40 - 周一
 * @Describe
 */
public class kafka_old_sink_12 {
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
        Table sensorTable = tEnv.fromDataStream(waterSensorStream);
        Table resultTable = sensorTable
                .where($("id").isEqual("sensor_1"))
                .select($("id"), $("timeStamp"), $("temperature"));

        //step-3 配置元数据
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("timeStamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE());

        tEnv.connect(new Kafka()
                        .version("universal")
                        .topic("sensor")
                        .sinkPartitionerRoundRobin() // 分区轮询
                        .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092"))
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("kafka_sink");

        resultTable.executeInsert("kafka_sink");


    }
}
