package com.atguigu.window;

import com.atguigu.source.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.runtime.operators.window.CountWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @ClassName FlinkDemo-TimeSlidingWindow
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日21:26 - 周三
 * @Describe 时间滑动窗口(SlidingProcessingTimeWindows), 使用不同的窗口计算器计算每个窗口中按key分区的平均值
 */
public class TimeSlidingWindow_2 {
    public static void main(String[] args) throws Exception {
        //step-1 准备环境 & 数据
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //这里需要用socket流来读取数据，因为如果读文件数据，没有15秒程序就结束了，窗口也消失了
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //sub-step-2.1 开启时间滑动全窗口函数(ProcessWindowFunction)----------------------------------
        dataStream.keyBy(SensorReading::getId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(3), Time.seconds(1)))
                /*
                 * Explain
                 * ProcessWindowFunction<SensorReading, Tuple2<String, Double>, String, TimeWindow>
                 * 泛型参数一:输入类型,这里是SensorReading
                 * 泛型参数二:输出类型,这里是Tuple2<String, Double>
                 * 泛型参数三:按keyBy分区后的key类型,这里key的类型是String
                 * 泛型参数四:窗口类型,这里是TimeWindow时间窗口,另外一个类型是GlobalTime
                 * */
                .process(new ProcessWindowFunction<SensorReading, Tuple2<String, Double>, String, TimeWindow>() {
                    //Attention 这里不能使用状态,状态没有窗口
                    @Override//s是就是分区的key
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
                });//.print("时间滑动全窗口函数1");
        /*
         * Prepare 测试数据
         *  sensor_1,1638948510,1
         *  sensor_1,1638948522,2
         *  sensor_1,1638948533,6
         *  sensor_1,1638948542,99
         *  sensor_13,1638948642,13
         *  sensor_13,1638948742,40.2
         *
         * Result 结果
         *  时间滑动全窗口> (sensor_1,27.0)
         *  时间滑动全窗口> (sensor_13,26.6)
         *
         * */

        //sub-step-2.2 开启时间滑动全窗口函数(WindowFunction)--------------------------------------
        dataStream.keyBy(SensorReading::getId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                /*
                 * Explain
                 * WindowFunction<SensorReading, Integer, String, TimeWindow>
                 * 泛型参数一:输入类型,这里是SensorReading
                 * 泛型参数二:输出类型,这里是Tuple2<String, Double>
                 * 泛型参数三:按keyBy分区后的key类型,这里key的类型是String
                 * 泛型参数四:窗口类型,这里是TimeWindow时间窗口,另外一个类型是GlobalTime
                 * */
                .apply(new WindowFunction<SensorReading, Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple2<String, Double>> out) throws Exception {
                        Double sum = 0.0D;
                        //将每个窗口的元素和叠加
                        for (SensorReading sensorReading : input) {
                            sum += sensorReading.getTemperature();
                        }
                        //计算每个窗口的个数
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        //计算平均值
                        out.collect(Tuple2.of(s, sum / count));
                    }
                });//.print("时间滑动全窗口函数2");

        /*
         * Result 测试数据同step2.1的结果一样
         * */


        //sub-step-2.3 开启时间滑动聚合窗口函数(ReduceFunction)------------------------------------
        dataStream.keyBy(SensorReading::getId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                /*
                 * Explain
                 * 1.ReduceFunction 的核心方法，将两个值合并为一个相同类型的值,reduce函数连续应用于组的所有值,直到只剩下一个值为止。
                 * 2.value1是上次聚合的结果值,所以遇到每个窗口的第一个元素时,这个函数不会进来
                 * 3.不能改变数据的类型,即你只能传一个SensorReading,但你想像上面那样传一个(id,avg)出去的话是不行的,因为返回类型和输入类型要一致
                 * */
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        //叠加温度值
                        value1.setTemperature(value1.getTemperature() + value2.getTemperature());
                        return value1;
                    }
                });//.print("时间滑动聚合窗口函数1");

        //sub-step-2.4 开启时间滑动聚合窗口函数(AggregateFunction)------------------------------------
        dataStream.keyBy(SensorReading::getId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                /*
                 * Explain
                 * AggregateFunction<SensorReading, Tuple2<Double, Integer>, Tuple2<String, Double>>
                 * 泛型参数一:输入数据类型
                 * 泛型参数二:中间累加值类型,这里我们使用Tuple2<id,聚合值,元素个数>,因为我们求的是平均值
                 * 泛型参数三:返回值,我们想得到的结果是Tuple2<id,平均值>
                 * */
                .aggregate(new AggregateFunction<SensorReading, Tuple3<String, Double, Integer>, Tuple2<String, Double>>() {
                    @Override
                    public Tuple3<String, Double, Integer> createAccumulator() {
                        return Tuple3.of("", 0.0D, 0);
                    }

                    @Override
                    public Tuple3<String, Double, Integer> add(SensorReading value, Tuple3<String, Double, Integer> accumulator) {
                        accumulator.f0 = value.getId();//因为是按key分区,所以id是一样的,取第一个就行
                        accumulator.f1 += value.getTemperature();//累积聚合值
                        accumulator.f2 += 1;//累积个数
                        return accumulator;
                    }

                    @Override
                    public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
                        //计算平均值
                        return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
                    }

                    @Override
                    public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
                        //聚合两个累加器的值
                        return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
                    }
                });//.print("时间滑动聚合窗口函数2");

        env.execute();
    }
}
