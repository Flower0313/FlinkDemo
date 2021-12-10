package com.atguigu.cep;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @ClassName FlinkDemo-basic_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月10日0:17 - 周五
 * @Describe 单个条件
 */
public class cep_single_1 {
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

        //Step-2.1 定义简单条件模式
        Pattern<SensorReading, SensorReading> simplePattern = Pattern
                .<SensorReading>begin("simple_start")//simple_start只是一个名称
                .where(new SimpleCondition<SensorReading>() {
                    @Override //筛选出名称是sensor_1
                    public boolean filter(SensorReading value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                }).oneOrMore();
                /*
                 * Explain 关于times的用法
                 *  当你设置.times(6)时,就表示sensor_1出现了六次就输出,不足6次的话是不会输出的,
                 *  当然sensor_1的出现是可以不连续的,只要总数达到了6次即可,相当于SQL中的count(id)>=6,
                 *  但这里会以k-v的形式输出,6条数据全部在value中,key就是你自己定义的simple_start
                 *
                 * Example
                 *  times(n)    出现n次
                 *  times(n).optional() 出现0次或n次
                 *  times(n,m)  出现n~m次(n<m)
                 *  times(n,m).greedy() 出现n~m,m+1,..
                 *  oneOrMore()   出现1次到多次
                 *  oneOrMore().greedy()  出现1到多次,并且尽可能的重复次数多
                 *  oneOrMore().optional()    出现0次到多次
                 *  timesOrMore(2)  出现2到多次
                 *  timesOrMore(2).greedy()  出现2到多次,并尽可能多
                 *  timesOrMore(2).optional()   出现0、2或多次
                 *  timesOrMore(2).optional().greedy()  出现0、2或多次,并尽可能多
                 *
                 *
                 * Result
                 *  循环模式 - oneOrMore   1次或多次
                 * 先输出1次的
                 * 1
                 * 30
                 * 99
                 * 101
                 * 再输出2次的
                 * 1 30
                 * 30 99
                 * 99 101
                 * 再输出3次的
                 * 1 30 99
                 * 30 99 101
                 * 再输出全部次数的
                 * 1 30 99 101
                 * */


        //Step-2.2 定义迭代条件模式
        Pattern<SensorReading, SensorReading> intervalPattern = Pattern
                .<SensorReading>begin("interval_start")
                .where(new IterativeCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value, Context<SensorReading> ctx) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                });


        //Step-2.3 使用简单的filter实现模式API的功能
        waterSensorStream.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading value) throws Exception {
                return "sensor_1".equals(value.getId()) || value.getTemperature() >= 35;
            }
        });//.print("filter");

        //Step-3 在原始流上应用刚刚定义的模式,其实模式就相当于高级filter,过滤出模式流
        PatternStream<SensorReading> simpleDS = CEP.pattern(waterSensorStream, simplePattern);
        PatternStream<SensorReading> intervalDS = CEP.pattern(waterSensorStream, intervalPattern);


        //Step-4.1 从模式流中获取匹配到的结果(PatternSelectFunction)
        /*
         * Explain
         *  数据不能直接调用print()
         *  Map<String, List<IN>> 其中这个Map中的key就是你的模式序列中每个模式的名称,也就是flower_start
         *  value是被接受的事件列表,IN是输入事件的类型,模式的输入事件按照时间戳进行排序。
         * */
        simpleDS
                .select(new PatternSelectFunction<SensorReading, String>() {
                    @Override
                    public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                        return pattern.toString();
                    }
                })
                .print("pattern");


        //Step-4.2 从模式流中获取匹配到的结果(PatternProcessFunction)
        /*
         * PatternProcessFunction<IN, OUT>
         *
         * Map<String, List<IN>> match, Context ctx, Collector<OUT> out
         * */
        intervalDS.process(new PatternProcessFunction<SensorReading, String>() {
            @Override//这个方法在每匹配到一个时都会调用,会按你之前分配的事件时间排序
            public void processMatch(Map<String, List<SensorReading>> match, Context ctx, Collector<String> out) throws Exception {
                out.collect(match.toString());
            }
        }).print();

        env.execute();
    }
}