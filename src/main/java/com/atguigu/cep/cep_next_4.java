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
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName FlinkDemo-cep_next_4
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月10日18:11 - 周五
 * @Describe 严格连续
 */
public class cep_next_4 {
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
                        .withTimestampAssigner((element, recordTimestamp) -> element.getTimeStamp()));

        //Step-2 条件组合
        Pattern<SensorReading, SensorReading> nextPattern = Pattern
                .<SensorReading>begin("next_start")//next_start只是一个名称
                .where(new IterativeCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading sensorReading, Context<SensorReading> context) throws Exception {
                        return "sensor_1".equals(sensorReading.getId());
                    }
                }).next("next_end").within(Time.seconds(10))
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return "sensor_13".equals(value.getId());
                    }
                });
        /*
         * Prepare-1 sensor_1.next(sensor_13) 过滤出sensor_1后面就是sensor_13的数据
         *
         *
         * {next_start=[SensorReading{id='sensor_1', timeStamp=1638948542000, temperature=99.0}]
         * next_end=[SensorReading{id='sensor_13', timeStamp=1638948642000, temperature=13.0}]}
         *
         * Prepare-2
         *  sensor_1,4,99
         *  sensor_13,15,22
         *
         * Example
         *  sensor_1.next(sensor_13).within(Time.seconds(10))
         *  必须是sensor_1后面的sensor_13在10秒内发生的,这里能识别到时间戳就是因为Step-1中指定了事件时间戳字段
         *
         * Result-2
         *  不输出,因为15-4=11 超过了10秒
         * */

        //Step-3 在流上注册模式
        PatternStream<SensorReading> nextDS = CEP.pattern(waterSensorStream, nextPattern);

        //Step-4 打印流
        nextDS.select(new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print("next_start");

        env.execute();
    }
}
