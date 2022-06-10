package com.holden.cep;

import com.holden.bean.SensorReading;
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
 * @ClassName FlinkDemo-cep_notNext_7
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月10日18:53 - 周五
 * @Describe
 */
public class cep_notNext_7 {
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
        Pattern<SensorReading, SensorReading> noNextPattern = Pattern
                .<SensorReading>begin("noNext_start")//next_start只是一个名称
                .where(new IterativeCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading sensorReading, Context<SensorReading> context) throws Exception {
                        return "sensor_1".equals(sensorReading.getId());
                    }
                }).notNext("noNext_end")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return "sensor_13".equals(value.getId());
                    }
                });
        /*
         * Attention 同id的数据时间戳不能相同,不然结果不能select出来,因为结果是按时间戳来排序的
         * Result 过滤出sensor_1不紧邻sensor_13的数据,输出结果按时间戳来排序
         **/

        //Step-3 在流上注册模式
        PatternStream<SensorReading> noNextDS = CEP.pattern(waterSensorStream, noNextPattern);

        //Step-4 打印流
        noNextDS.select(new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print("noNext_start");

        env.execute();
    }
}
