package com.atguigu.window;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-countwindow_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月22日11:08 - 周一
 * @Describe 计数窗口, 对于计数窗口来说滚动窗口和滑动窗口的区别就是传参的个数
 */
public class CountWindow_3 {
    public static void main(String[] args) throws Exception {
        //step-1 准备环境 & 数据
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        //DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //step-1.基于计数的滑动"聚合"窗口:按id分区统计平均温度值
        dataStream.keyBy(SensorReading::getId)
                //窗口大小为3,滑动步长为1 attention 若只传递一个参数就代表是滚动计数窗口
                .countWindow(3, 1)
                //输入类型是SensorReading,输出类型是平均值Double
                //中间的参数使用元组，第一个位置存温度总和，第二个位置存个数
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<>(0.0, 0);//中间累加器的初始值
                    }

                    @Override//参数一是输入元素，参数二是暂存累加器
                    public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
                        //第一个Tuple2的元素是累积的温度值，第二个Tuple2的元素是个数
                        return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        //将两个Tuple2合并在一起
                        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
                    }
                }).print();

        env.execute();
    }
}
