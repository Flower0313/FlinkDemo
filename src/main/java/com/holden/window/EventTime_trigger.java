package com.holden.window;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * @ClassName FlinkDemo-eventtime_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月22日19:26 - 周一
 * @Describe 事件时间Event Time(1.12后默认)
 */
public class EventTime_trigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))//延迟时间
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {//这里将自定义SensorReading类型传入进去
                    @Override//抽取时间戳
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimeStamp() * 1000L;
                    }
                }));


        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .trigger(new MyTrigger())
                .process(new ProcessWindowFunction<SensorReading, SensorReading, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<SensorReading, SensorReading, String, TimeWindow>.Context context, Iterable<SensorReading> elements, Collector<SensorReading> out) throws Exception {
                        Iterator<SensorReading> iterator = elements.iterator();
                        out.collect(iterator.next());
                    }
                });

        sumStream.print();


        env.execute();
    }

    //自定义触发器
    public static class MyTrigger extends Trigger<SensorReading,TimeWindow>{

        @Override//来了一条数据做什么动作
        public TriggerResult onElement(SensorReading element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            System.out.println("数据来了");
            return TriggerResult.FIRE;
        }

        @Override//对应处理时间
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override//对应事件时间
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return null;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }
}
