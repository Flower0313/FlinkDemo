package com.atguigu.table.api;

import com.atguigu.source.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-file_old_sink_10
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日17:55 - 周一
 * @Describe
 */
public class file_old_sink_10 {
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


        //step-3 配置元数据
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("timeStamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE());

        tEnv.connect(new FileSystem().path("output/sink.txt"))
                .withFormat(new Csv().fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("sensor_sink");

        resTable.executeInsert("sensor_sink");

    }
}
