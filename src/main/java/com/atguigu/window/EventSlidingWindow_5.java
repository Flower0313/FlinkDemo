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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

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

        /*
         * Attention
         * 1.这个参数是用于间隔生成水位线的时间,默认是200毫秒生成一次
         * 2.若你设置了50000L,也就是50秒生成一次,那么你窗口大小设置为15,延迟时间设置为2,就算你输出了
         *   sensor_1,1,1和sensor_1,17,35.7后结果也不会立马出来,因为还没满足要求的水位线过来,因为Event
         *   窗口不会看数据上的时间,它只会看水位线的时间戳,也就是数据时间戳-延迟时间,所以还没达到50秒,第二次
         *   水位线还没有生成,所以就算你数据到了,也不会有任何输出,只有50秒到了生成了第二次17-2=15秒的水位线
         *   就会输出第一个窗口[0,15)的值。
         * */

        //可以通过这个参数设置生成水位线的时间,且时间是处理时间《
        env.getConfig().setAutoWatermarkInterval(50000L);//设置自动水位线默认200毫秒
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
                            @Override//抽取SensorReading中时间戳属性然后-延迟时间=水位线
                            public long extractTimestamp(SensorReading element, long recordTimestamp) {
                                //获取事件自带的时间戳来作为水位线,*1000是必须要加的,因为要变成毫秒
                                return element.getTimeStamp() * 1000L;
                            }
                        }));


        //step-3 开滑动事件时间窗口,若还使用计数窗口,那水位线不是没有意义吗
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
                });//.print("基于事件时间的滑动窗口");

        //step-4 开滚事件时间窗口
        watermarks.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .apply(new WindowFunction<SensorReading, SensorReading, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) throws Exception {
                        for (SensorReading sensorReading : input) {
                            out.collect(sensorReading);
                        }
                    }
                }).print();

        env.execute();
    }
}
