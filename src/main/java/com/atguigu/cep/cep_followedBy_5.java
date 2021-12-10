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
 * @ClassName FlinkDemo-cep_followedBy_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月10日18:24 - 周五
 * @Describe 松散连续
 */
public class cep_followedBy_5 {
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
                        .withTimestampAssigner((e, r) -> e.getTimeStamp()));

        //Step-2 条件组合
        Pattern<SensorReading, SensorReading> followedPattern = Pattern
                .<SensorReading>begin("followedBy_start")//followedBy_start只是一个名称
                .where(new IterativeCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading sensorReading, Context<SensorReading> context) throws Exception {
                        return "sensor_1".equals(sensorReading.getId());
                    }
                }).followedBy("end")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return "sensor_13".equals(value.getId());
                    }
                });
        /*
        * Prepare-1
        * sensor_1,1638948510,1
        * sensor_2,222,11
        * sensor_1,1638948533,30
        * sensor_1,1638948542,99
        * sensor_13,1638948642,13
        * sensor_13,222,15
        *
        * Result-1注意这里以followedBy后的数据为界限,可以得出只会匹配followedBy的后第一个满足的数据,
        *  且这条满足的数据前面的数据可以允许出现不匹配的情况,直接穿透能匹配上的数据
        * sensor_1,1638948510,1
        * sensor_13,1638948642,13
        *
        * sensor_1,1638948533,30
        * sensor_13,1638948642,13
        *
        * sensor_1,1638948542,99
        * sensor_13,1638948642,13
        *
        * Prepare-2
        * sensor_1,1638948510,1
        * sensor_1,1638948533,30
        * sensor_1,1638948542,99
        * sensor_2,13421412,223
        * sensor_13,1638948642,13
        *
        * Result-2 中间隔了一条sensor_2这样就啥都匹配不上,
        *  也就是说明followedBy后面满足条件的数据上一条紧邻的必须是满足前面条件的数据
        *
        * */

        //Step-3 在流上注册模式
        PatternStream<SensorReading> followedByDS = CEP.pattern(waterSensorStream, followedPattern);

        //Step-4 打印流
        followedByDS.select(new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print("followedBy_start");

        env.execute();
    }
}
