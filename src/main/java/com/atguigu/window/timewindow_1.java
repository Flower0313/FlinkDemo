package com.atguigu.window;

import com.atguigu.source.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-window_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日21:03 - 周日
 * @Describe 时间窗口
 */
public class timewindow_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //这里需要用socket流来读取数据，因为如果读文件数据，没有15秒程序就结束了，窗口也消失了
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
        //1.增量聚合窗口:方式一
        dataStream.keyBy(SensorReading::getId)//虽然分了区，但还是一个线程，每个分区都会输出自己的统计值
                //加入你设置了多并行度，那么每个分区都可以分配一个线程并行运行，但他们数据是可以共通的
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //第一个参数是输入的数据类型;第二个是累加器;第三个是输出数据类型
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
                }).print();
        //dataStream.keyBy(SensorReading::getId).window(EventTimeSessionWindows.withGap(Time.minutes(5)));//方式二
        //dataStream.keyBy(SensorReading::getId).countWindow(5);


        //2.全局聚合窗口
        dataStream.keyBy(SensorReading::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<SensorReading, Tuple3<String,Long,Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String,Long,Integer>> out) throws Exception {
                        String id = s.split(",")[0];//获取到id
                        Long windowEnd = window.getEnd();//每个窗口结束的时间戳
                        Integer count = IteratorUtils.toList(input.iterator()).size();//每组的统计个数
                        //相比增量窗口，可以拿到更多信息
                        out.collect(new Tuple3<>(id,windowEnd,count));
                    }
                });
        //从这可以看出，分区后的数据统计值是分区各算个的，但是他们还是处于一个线程中的，不是并行


        env.execute();
    }
}
