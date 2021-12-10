package com.atguigu.window;

import com.atguigu.bean.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName FlinkDemo-CountEventWindow_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月09日9:02 - 周四
 * @Describe 基于事件时间的滑动窗口
 */
public class EventSlidingWindow_5 {
    public static void main(String[] args) throws Exception {
        //step-1 准备环境 & 数据
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        //step-2 打上水位线,设置延迟时间
        //explain 这里在标记使用哪个字段作为水位线
        DataStream<SensorReading> watermarks = dataStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))//延迟时间
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override//抽取SensorReading中时间戳属性作为水位线
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        //获取事件自带的时间戳来作为水位线,*1000是必须要加的,因为要变成毫秒
                        return element.getTimeStamp() * 1000L;
                    }
                }));


        //step-3 开滚事件时间窗口,若还使用计数窗口,那水位线不是没有意义吗
        watermarks.keyBy(SensorReading::getId)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(10)))
                .process(new ProcessWindowFunction<SensorReading, Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<SensorReading, Tuple2<String, Double>, String, TimeWindow>.Context context, Iterable<SensorReading> elements, Collector<Tuple2<String, Double>> out) throws Exception {
                        //注意每个key的每个窗口都会执行一下process方法,最终结果就是输出的是每个窗口的平均值
                        Double sum = 0.0D;
                        //每组窗口中的个数,注意此时窗口是按key分区的
                        Integer count = IteratorUtils.toList(elements.iterator()).size();
                        //获取每个窗口中数据集合,每个窗口累加到sum
                        for (SensorReading element : elements) {
                            sum += element.getTemperature();
                        }
                        out.collect(Tuple2.of(s, sum / count));
                    }
                }).print("基于事件时间的滑动窗口");

        env.execute();
    }
}
