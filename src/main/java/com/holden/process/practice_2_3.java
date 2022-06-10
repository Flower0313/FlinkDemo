package com.holden.process;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName FlinkDemo-practice_2_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日16:35 - 周二
 * @Describe
 */
public class practice_2_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimeStamp() * 1000L;
                    }
                }));

        dataStream.keyBy(SensorReading::getId)
                        .process(new ProcessFunction<SensorReading, String>() {
                            @Override
                            public void processElement(SensorReading value, ProcessFunction<SensorReading, String>.Context ctx, Collector<String> out) throws Exception {

                            }
                        });
        dataStream.process(new ProcessFunction<SensorReading, String>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
                ctx.timerService().currentWatermark();
                System.out.println("当前水位线:" + ctx.timerService().currentWatermark());
                System.out.println("当前时间戳:" + ctx.timestamp());
                System.out.println("------------------------------------------");
            }
        }).print();


        env.execute();
    }
}
