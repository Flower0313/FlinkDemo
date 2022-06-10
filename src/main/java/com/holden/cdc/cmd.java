package com.holden.cdc;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-cmd
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月08日14:24 - 周三
 * @Describe
 */
public class cmd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);


        DataStreamSource<String> inputStream313 = env.socketTextStream("hadoop102", 31313);
        DataStreamSource<String> inputStream8 = env.socketTextStream("hadoop102", 8888);

        DataStream<SensorReading> dataStream13 = inputStream313.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimeStamp() * 1000L;
                    }
                }));

        DataStream<SensorReading> dataStream8 = inputStream8.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimeStamp() * 1000L;
                    }
                }));

        Table sensorTable13 = tEnv.fromDataStream(dataStream13, $("id"), $("timeStamp").as("ts"), $("temperature").as("temp"));
        Table sensorTable8 = tEnv.fromDataStream(dataStream8, $("id"), $("timeStamp").as("ts"), $("temperature").as("temp"));
        tEnv.createTemporaryView("sensor13", sensorTable13);
        tEnv.createTemporaryView("sensor8", sensorTable8);

        Table resultTable = tEnv.sqlQuery(
                "select sensor13.id as a,count(1) as nums from sensor13 join sensor8 on sensor13.id=sensor8.id");

        resultTable.execute().print();
        env.execute();
    }
}
