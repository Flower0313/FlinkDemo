package com.holden.table.api;

import com.holden.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName FlinkDemo-BasicUse_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月28日21:46 - 周日
 * @Describe
 */
public class StreamToData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //step-1 DataStream -> Table
        Table sensorTable1 = tEnv.fromDataStream(inputStream);
        Table sensorTable2 = tEnv.fromDataStream(inputStream, "id,timeStamp as ts,temperature");

        //step-2 Table -> DataStream
        DataStream<Row> resStream1 = tEnv.toAppendStream(sensorTable1, Row.class);
        DataStream<Tuple2<Boolean, Row>> resStream2 = tEnv.toRetractStream(sensorTable1, Row.class);

        //step-3 基于DataStream临时视图
        tEnv.createTemporaryView("sensorView1", dataStream);
        tEnv.createTemporaryView("sensorView1", dataStream, "id,temperature as ts");

        //step-4 基于Table临时视图
        tEnv.createTemporaryView("sensorView2", sensorTable2);

        env.execute();
    }
}
