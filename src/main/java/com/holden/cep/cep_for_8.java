package com.holden.cep;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName FlinkDemo-cep_for_8
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月10日19:27 - 周五
 * @Describe 单个循环模式
 */
public class cep_for_8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-1 原始数据源
        DataStream<SensorReading> waterSensorStream = env
                .readTextFile("input/sensor.txt")
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new SensorReading(split[0],
                                Long.parseLong(split[1]),
                                Double.valueOf(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((e, r) -> e.getTimeStamp() * 1000L));

        //Step-2 定义模式
        Pattern<SensorReading, SensorReading> forPattern = Pattern
                .begin(//模式组
                        Pattern.<SensorReading>begin("start")
                                .where(new SimpleCondition<SensorReading>() {
                                    @Override //筛选出名称是sensor_1
                                    public boolean filter(SensorReading value) throws Exception {
                                        return "sensor_1".equals(value.getId());
                                    }
                                })).timesOrMore(2)
                .consecutive()
                .within(Time.seconds(2));//这个表示连续两次的时间差不超过3秒
        //.within(Time.seconds(1));

        KeyedStream<SensorReading, String> keyedStream = waterSensorStream.keyBy(SensorReading::getId);

        Pattern<SensorReading, SensorReading> chaoshi = Pattern.<SensorReading>begin("begin")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                }).times(2).consecutive();

        PatternStream<SensorReading> pattern = CEP.pattern(keyedStream, chaoshi);


        //Step-3 在流上应用模式
        PatternStream<SensorReading> forStream = CEP.pattern(waterSensorStream, forPattern);

        //Step-4 从流出输出
        pattern
                .select(new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print("pattern");

        env.execute();
    }
}
