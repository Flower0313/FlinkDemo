package com.atguigu.cep;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName FlinkDemo-fucker
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月16日16:53 - 周四
 * @Describe
 */
public class fucker {
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

        KeyedStream<SensorReading, String> keyedStream = waterSensorStream.keyBy(SensorReading::getId);
        //Step-2 定义模式
        //模式组
        Pattern<SensorReading, SensorReading> withinPattern = Pattern.<SensorReading>begin("one")
                .where(new SimpleCondition<SensorReading>() {
                    @Override //筛选出名称是sensor_1
                    public boolean filter(SensorReading value) throws Exception {
                        return value.getTemperature() >= 1;
                    }
                }).times(2).consecutive().within(Time.seconds(5));//定义在这和定义在next("2")后面一样都是作用在2号模式组上


        //Step-3 在流上应用模式
        PatternStream<SensorReading> forStream = CEP.pattern(keyedStream, withinPattern);
        OutputTag<SensorReading> timeOutTag = new OutputTag<SensorReading>("TimeOut") {
        };

        /*forStream
                .select(new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print("pattern");*/

        SingleOutputStreamOperator<SensorReading> selectDS = forStream.select(timeOutTag,
                new PatternTimeoutFunction<SensorReading, SensorReading>() {
                    @Override
                    public SensorReading timeout(Map<String, List<SensorReading>> pattern, long timeoutTimestamp) throws Exception {
                        /*for (Map.Entry<String, List<SensorReading>> entry : pattern.entrySet()) {
                            System.out.println("超时数据:" + entry.getKey() + ">>" + entry.getValue());
                        }*/
                        return pattern.get("one").get(0);
                    }
                }, new PatternSelectFunction<SensorReading, SensorReading>() {
                    @Override
                    public SensorReading select(Map<String, List<SensorReading>> pattern) throws Exception {
                        /*for (Map.Entry<String, List<SensorReading>> entry : pattern.entrySet()) {
                            System.out.println("正常数据:" + entry.getKey() + ">>" + entry.getValue());
                        }*/
                        return pattern.get("one").get(0);
                    }
                });

        DataStream<SensorReading> sideOutput = selectDS.getSideOutput(timeOutTag);

        sideOutput.print("超时数据>>>");
        DataStream<SensorReading> unionDS = selectDS.union(sideOutput);
        unionDS.print("总数据量>>>");

        env.execute();
    }
}
