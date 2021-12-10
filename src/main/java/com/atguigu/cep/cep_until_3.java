package com.atguigu.cep;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName FlinkDemo-cep_basic_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月10日16:40 - 周五
 * @Describe 停止条件
 */
public class cep_until_3 {
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

        //Step-2 条件组合
        Pattern<SensorReading, SensorReading> untilPattern = Pattern
                .<SensorReading>begin("until_start")//until_start只是一个名称
                .where(new IterativeCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading sensorReading, Context<SensorReading> context) throws Exception {
                        return "sensor_1".equals(sensorReading.getId());
                    }
                }).oneOrMore()//until只能用在循环上
                .until(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return value.getTemperature() >= 40;
                    }
                });
        /*
         * Result 基于oneOrMore的until结果,遇到40的停止了
         * 1
         * 1 30
         *
         * 30
         * */

        //Step-3 在流上注册模式
        PatternStream<SensorReading> untilDS = CEP.pattern(waterSensorStream, untilPattern);

        //Step-4 打印流
        untilDS.select(new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print("until-pattern");

        env.execute();
    }
}
