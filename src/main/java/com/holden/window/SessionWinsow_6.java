package com.holden.window;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
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
        }).assignTimestampsAndWatermarks(WatermarkStrategy//explain 告诉flink哪个是时间戳,到时候需要用这个时间戳和水位线进行对比
                //乱序使用forBoundedOutOfOrderness，顺序使用forMonotonousTimestamps()
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))//延迟时间
                //x就是SensorReading类型的,y就是Long类型的recordTimestamp,以毫秒为单位,所以要*1000
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {//这里将自定义SensorReading类型传入进去
                    @Override//抽取时间戳
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        //获取事件自带的时间戳来作为水位线,*1000是必须要加的
                        return element.getTimeStamp() * 1000L;
                    }
                }));

        //Attention-1 静态事件时间会话窗口
        WindowedStream<SensorReading, String, TimeWindow> window1 = dataStream.keyBy(SensorReading::getId).window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        //Attention-2 动态事件时间会话窗口
        WindowedStream<SensorReading, String, TimeWindow> window3 = dataStream.keyBy(SensorReading::getId).window(EventTimeSessionWindows.withGap(Time.seconds(5)));
        /*
         * 事件时间窗口window3
         * s1,1,1
         * s1,7,1 然后就输出[1,6)窗口的数据,因为1+5=6(起始时间+会话时间),
         * 然后再输入时间戳为7的数据,超过了这个会话窗口的间隔(7-1=6>5,证明5前面的窗口已经5秒没数据了),所以输出。
         * */

        /*
         * Attention-3 动态处理时间会话窗口
         * 动态会话窗口划分的规则就是，
         * 如果下一条数据的时间和上一条数据的间隔不超过上一条数据的会话时间戳的话，就同属于一个窗口，而且窗口的大小就是
         * [ 这个窗口中最早的数据进入时间，最早的数据进入时间+窗口中数据最大的时间戳 )
         * */
        WindowedStream<SensorReading, String, TimeWindow> window2 = dataStream.keyBy(SensorReading::getId)
                .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<SensorReading>() {
                    @Override
                    public long extract(SensorReading element) {
                        return element.getTimeStamp() * 1000;
                    }
                }));

        //Attention-4 动态事件时间会话窗口
        WindowedStream<SensorReading, String, TimeWindow> window4 = dataStream.keyBy(SensorReading::getId).window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<SensorReading>() {
            @Override
            public long extract(SensorReading element) {
                //抽取时间戳当成会话时间间隔
                return element.getTimeStamp() * 1000L;
            }
        }));

        window4.process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<SensorReading, String, String, TimeWindow>.Context context, Iterable<SensorReading> elements, Collector<String> out) throws Exception {
                String msg =
                        "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                + elements.spliterator().estimateSize() + "条数据";
                out.collect(msg);

            }
        }).print();
        /*
        * window4动态事件时间会话窗口
        * Example-1 输入:
        * s1,5,1
        * s1,10,1 -- 没有达到11,没有触发此会话窗口
        * s1,16,1 -- 没有达到21,没有触发此会话窗口
        * s1,33,1 -- 超过了16*2=32,触发会话窗口
        * attention 这里的窗口会话的间隔是取最大时间戳,而开始时间就是这个会话窗口最先进来数据的时间戳(最小时间戳)
        *
        * Result-1 输出:
        * [5,32) 一共3条
        *
        * Example-2输入:
        * s1,5,1
        * s1,4,1
        * s1,8,1
        * s1,17,1
        *
        * Result-2 输出:
        * [4,16) 一共3条
        * */

        env.execute();

    }
}
