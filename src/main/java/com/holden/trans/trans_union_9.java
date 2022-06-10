package com.holden.trans;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @ClassName FlinkDemo-trans_union_9
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月04日20:24 - 周六
 * @Describe union
 */
public class trans_union_9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//并行度1，只开放一个slot
        /*
         * Explain 这里是给两条流先打水位线再union
         * 1.可以超过2个或2个以上的流进行union,产生一个包含所有流的新流
         * 2.多个流的类型必须一样
         * 3.若给这两个流分别打上水位线,那么触发水位线需要每个流中都达到窗口才能触发,不然不会有数据输出
         *   比如这里设置10秒的窗口,那么one流和two流都需要达到10秒的时间戳才能输出数据
         * 4.若one流设置2秒的延迟,则触发窗口的时间就是one流需要12秒,而two流只需要10秒就能触发,但输出的都是[0,10)的数据
         *
         * Conclusion:
         *  1.当这两条流的水位线不同时,当两个流都达到了自己的水位线时,就可以输出属于窗口中的数据
         *  比如one延迟2秒,two流不延迟,开10秒的窗口,若只有一边达到水位线是不会有输出的,
         *  只有one流达到12的水位线 & two流达到10的水位线,这时候才会输出[0,10)窗口的数据。
         *  2.若是union再打水位线,那就是统一了,这样延迟时间就只有一个
         * */

        DataStreamSource<String> one = env.socketTextStream("hadoop102", 31313);
        DataStream<SensorReading> oneDS = one.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((ele, record) -> {
                    return ele.getTimeStamp() * 1000L;
                }));


        DataStreamSource<String> two = env.socketTextStream("hadoop102", 8888);
        DataStream<SensorReading> twoDS = two.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((ele, record) -> {
                    return ele.getTimeStamp() * 1000L;
                }));


        DataStream<SensorReading> union = oneDS.union(twoDS);
        union.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<SensorReading, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<SensorReading, String, String, TimeWindow>.Context ctx, Iterable<SensorReading> elements, Collector<String> out) throws Exception {
                        for (SensorReading element : elements) {
                            out.collect(element + ",[" + ctx.window().getStart() + "," + ctx.window().getEnd() + ")");
                        }
                    }
                }).print(">>>");

        env.execute();
    }
}
