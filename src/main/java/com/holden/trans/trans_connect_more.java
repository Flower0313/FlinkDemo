package com.holden.trans;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @ClassName FlinkDemo-trans_union_9
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月04日20:24 - 周六
 * @Describe connect+keyBy
 */
public class trans_connect_more {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//并行度1，只开放一个slot


        DataStreamSource<String> one = env.socketTextStream("hadoop102", 31313);
        DataStream<SensorReading> oneDS = one.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((ele, record) -> {
                    return ele.getTimeStamp() * 1000L;
                }));


        DataStreamSource<String> two = env.socketTextStream("hadoop102", 8888);
        DataStream<SensorReading> twoDS = two.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((ele, record) -> {
                    return ele.getTimeStamp() * 1000L;
                }));


        //connect不能使用keyBy
        //ConnectedStreams<SensorReading, SensorReading> connect = oneDS.connect(twoDS);


        env.execute();
    }
}
