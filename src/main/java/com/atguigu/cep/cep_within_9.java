package com.atguigu.cep;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.metrics2.impl.MsInfo.Context;

/**
 * @ClassName FlinkDemo-cep_within_9
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月11日13:47 - 周六
 * @Describe
 */
public class cep_within_9 {
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
        //模式组
        Pattern<SensorReading, SensorReading> withinPattern = Pattern.<SensorReading>begin("1")
                .where(new SimpleCondition<SensorReading>() {
                    @Override //筛选出名称是sensor_1
                    public boolean filter(SensorReading value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                }).next("2")
                .where(new SimpleCondition<SensorReading>() {
                    @Override
                    public boolean filter(SensorReading value) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                }).within(Time.seconds(4));//定义在这和定义在next("2")后面一样都是作用在2号模式组上


        //Step-3 在流上应用模式
        PatternStream<SensorReading> forStream = CEP.pattern(waterSensorStream, withinPattern);

        //Step-4 从模式流中正常输出
        forStream.select(new PatternSelectFunction<SensorReading, String>() {
            @Override
            public String select(Map<String, List<SensorReading>> pattern) throws Exception {
                return pattern.toString();
            }
        });

        //
        SingleOutputStreamOperator<String> result = forStream
                .process(new MyPatternProcessOut());

        result.getSideOutput(new OutputTag<SensorReading>("timeout") {
        }).print("超时数据");
        result.print("正常数据");


        env.execute();
    }

    /**
     * 自定义 超时and正常 输出类
     */
    public static class MyPatternProcessOut extends PatternProcessFunction<SensorReading, String> implements TimedOutPartialMatchHandler<SensorReading> {
        @Override
        public void processMatch(Map<String, List<SensorReading>> match, PatternProcessFunction.Context ctx, Collector<String> out) throws Exception {
            out.collect(match.toString());
        }

        @Override
        public void processTimedOutMatch(Map<String, List<SensorReading>> match, PatternProcessFunction.Context ctx) throws Exception {
            //Attention 输出到侧输出流
            ctx.output(new OutputTag<String>("timeout") {
            }, match.toString());
            //System.out.println("超时数据:" + match.toString());
        }
    }
}