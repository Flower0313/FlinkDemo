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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName FlinkDemo-cep_patternGroup_10
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月17日16:08 - 周五
 * @Describe
 */
public class cep_patternGroup_10 {
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
                                Long.parseLong(split[1]) * 1000L,
                                Double.valueOf(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp()));

        Pattern<SensorReading, SensorReading> pattern = Pattern
                .begin(Pattern
                        .<SensorReading>begin("start")
                        .where(new SimpleCondition<SensorReading>() {
                            @Override
                            public boolean filter(SensorReading value) throws Exception {
                                return "sensor_1".equals(value.getId());
                            }
                        })
                        .next("next")
                        .where(new SimpleCondition<SensorReading>() {
                            @Override
                            public boolean filter(SensorReading value) throws Exception {
                                return "sensor_2".equals(value.getId());
                            }
                        }))
                .times(2);

        PatternStream<SensorReading> untilDS = CEP.pattern(waterSensorStream, pattern);

        untilDS.select(new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print("pattern-group");
        env.execute();
    }
}
