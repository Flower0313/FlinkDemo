package com.atguigu.window;

import com.atguigu.source.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName FlinkDemo-window_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日21:03 - 周日
 * @Describe 时间滚动窗口
 */
public class TimeTumblingWindow_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.getConfig().setAutoWatermarkInterval(200L);//设置自动生成水印的时间间隔


        //这里需要用socket流来读取数据，因为如果读文件数据，没有15秒程序就结束了，窗口也消失了
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        //step-1.基于处理时间的滚动"聚合"窗口(AggregateFunction)
        dataStream.keyBy(SensorReading::getId)//虽然分了区，但还是一个线程，每个分区都会输出自己的统计值
                //加入你设置了多并行度，那么每个分区都可以分配一个线程并行运行，但他们数据是可以共通的
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //第一个参数是输入的数据类型;第二个是中间累加器;第三个是输出数据类型
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;//累加器的初始值
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;//直接累加，个数加1
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        System.out.println("niupi");
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;//涉及分区合并，这里不涉及分区合并
                    }
                });

        //dataStream.keyBy(SensorReading::getId).window(EventTimeSessionWindows.withGap(Time.minutes(5)));//方式二
        //dataStream.keyBy(SensorReading::getId).countWindow(5);

        //step-2.基于处理时间的滚动的"全"窗口(WindowFunction)
        dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = s.split(",")[0];//获取到id
                        Long windowEnd = window.getEnd();//每个窗口结束的时间戳
                        Integer count = IteratorUtils.toList(input.iterator()).size();//每组的统计个数
                        //attention 相比增量窗口，可以拿到更多信息
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                }).print();
        //从这可以看出，分区后的数据统计值是分区各算个的，但是他们还是处于一个线程中的，不是并行

        //3.其他API
        //定义标签
        OutputTag<SensorReading> lateTag = new OutputTag<SensorReading>("late"){};
        //这里不能替换DataStream类，因为只有这个类有getSideOutput方法
        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //.trigger()触发器
                //.evictor()移除器
                //延迟一分钟，到了该关闭窗口的时候不关闭，而是继续再等1分钟
                //体现就是到了9:00时立马将窗口收集到的结果输出，然后继续等1分钟，后1分钟来的数据就在之前的结果上进行更新
                .allowedLateness(Time.minutes(1))
                //若1分钟后窗口真的关闭后，还有迟到的数据话就直接扔到侧输出流中去,等于是做了两层保证，加上watermark可以做三层保证
                .sideOutputLateData(lateTag)
                .sum("temperature");

        //取出正常窗口的数据
        //sumStream.print();
        //取出迟到后被写入到侧输出流数据
        //sumStream.getSideOutput(lateTag).print("late");
        /*
        * Q&A!
        * Q：如何定义迟到数据?
        * A：迟到是一个相对概念，假设一个数据在8-9点产生，所以这个数据本身就是属于8-9点这个窗口的，
        *    但它9-10点才过来，所以它算一个迟到数据而不是算9-10点的窗口数据。
        * */

        env.execute();
    }
}




























