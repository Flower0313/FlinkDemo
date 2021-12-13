package com.atguigu.window;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName FlinkDemo-SessionWinsow_6
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月13日14:36 - 周一
 * @Describe 会话窗口
 */
public class SessionWinsow_6 {
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

        //Attention 静态会话窗口
        //WindowedStream<SensorReading, String, TimeWindow> window = dataStream.keyBy(SensorReading::getId).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        /*
         * Attention 动态会话窗口,不区分时间时间和处理时间,一律皆以处理时间为准
         * 动态会话窗口不区分事件时间和处理时间，因为它都是取当前数据进来的处理事件，动态会话窗口划分的规则就是，
         * 如果下一条数据的时间和上一条数据的间隔不超过上一条数据的会话时间戳的话，就同属于一个窗口，而且窗口的大小就是
         * [ 这个窗口中最早的数据进入时间，最早的数据进入时间+窗口中数据最大的时间戳 )
         * */
        WindowedStream<SensorReading, String, TimeWindow> window = dataStream.keyBy(SensorReading::getId)
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<SensorReading>() {
                    @Override
                    public long extract(SensorReading element) {
                        return element.getTimeStamp() * 1000;
                    }
                }));

        window.process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {

            @Override
            public void process(String s, ProcessWindowFunction<SensorReading, String, String, TimeWindow>.Context context, Iterable<SensorReading> elements, Collector<String> out) throws Exception {
                String msg =
                        "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                + elements.spliterator().estimateSize() + "条数据";
                out.collect(msg);

            }
        }).print();

        env.execute();

    }
}
