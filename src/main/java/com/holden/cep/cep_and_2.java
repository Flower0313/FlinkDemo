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

import static com.holden.common.CommonEnv.SENSOR;

/**
 * @ClassName FlinkDemo-cep_basic_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月10日16:32 - 周五
 * @Describe 组合条件
 */
public class cep_and_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-1 原始数据源
        DataStream<SensorReading> waterSensorStream = env
                .readTextFile(SENSOR)
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

        //Step-2.1 组合条件(And条件)
        Pattern<SensorReading, SensorReading> mutiAndPattern = Pattern
                .<SensorReading>begin("start")//start只是一个名称
                .where(new SimpleCondition<SensorReading>() {
                    @Override //筛选出名称是sensor_1
                    public boolean filter(SensorReading value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                }).where(new SimpleCondition<SensorReading>() {
                    @Override //在第一个条件的基础下再筛选出温度是偶数的
                    public boolean filter(SensorReading value) throws Exception {
                        return value.getTemperature() % 2 == 0;
                    }
                });

        //Step-2.2 组合条件(Or条件)
        Pattern<SensorReading, SensorReading> mutiOrPattern = Pattern
                .<SensorReading>begin("start")//start只是一个名称
                .where(new SimpleCondition<SensorReading>() {
                    @Override //筛选出名称是sensor_1
                    public boolean filter(SensorReading value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                }).or(new SimpleCondition<SensorReading>() {
                    @Override //或者筛选出sensor_3
                    public boolean filter(SensorReading value) throws Exception {
                        return "sensor_3".equals(value.getId());
                    }
                });


        //Step-3 在流上注册模式
        PatternStream<SensorReading> mutiAndDS = CEP.pattern(waterSensorStream, mutiAndPattern);
        PatternStream<SensorReading> mutiOrDS = CEP.pattern(waterSensorStream, mutiOrPattern);

        //Step-4 打印流
        mutiOrDS.select(new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print("");

        env.execute();
    }
}
