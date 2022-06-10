package com.holden.trans;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName FlinkDemo-trans_union_11
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日21:29 - 周五
 * @Describe
 */
public class trans_union_11 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //若有三个并行度,那么12的水位线要执行三次才会有结果,而且union并不会聚合分区,也会有多个并行的union,只有keyBy才会在同一分区
        env.setParallelism(1);//并行度1，只开放一个slot

        //Step-1 先不打水位线,这是先union再打水位线,看看什么情况
        DataStreamSource<String> one = env.socketTextStream("hadoop102", 31313);
        DataStream<SensorReading> oneDS = one.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStreamSource<String> two = env.socketTextStream("hadoop102", 8888);
        DataStream<SensorReading> twoDS = two.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<SensorReading> union = oneDS.union(twoDS);
        SingleOutputStreamOperator<SensorReading> streamOperator = union
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((ele, record) -> {
                            return ele.getTimeStamp() * 1000L;
                        }));

        //两条流共用一条水位线,就相当于一条流的一个水位线一样的效果
        streamOperator.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {
                    @Override
                    public void process(String element, ProcessWindowFunction<SensorReading, String, String, TimeWindow>.Context ctx, Iterable<SensorReading> elements, Collector<String> out) throws Exception {
                        out.collect(element + ",[" + ctx.window().getStart() + "," + ctx.window().getEnd() + ")");
                    }
                }).print();

        env.execute();
    }
}
