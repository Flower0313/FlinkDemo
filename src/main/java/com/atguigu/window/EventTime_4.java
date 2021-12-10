package com.atguigu.window;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @ClassName FlinkDemo-eventtime_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月22日19:26 - 周一
 * @Describe 事件时间Event Time(1.12后默认)
 */
public class EventTime_4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设置为4后，由于source不受这个影响还是1，那么它将数据轮询给4个map，4个map中的数据通过keyBy就基于hash打乱了
        env.setParallelism(1);//不同分区不共用一个窗口时间,但同分区不同组的还是共用一个窗口时间
        //env.getConfig().setAutoWatermarkInterval(50L);//设置自动水位线默认200毫秒

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy//explain 告诉flink哪个是时间戳
                //乱序使用forBoundedOutOfOrderness，顺序使用forMonotonousTimestamps()
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))//延迟时间
                //x就是SensorReading类型的,y就是Long类型的recordTimestamp,以毫秒为单位,所以要*1000
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {//这里将自定义SensorReading类型传入进去
                    @Override//抽取时间戳
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        //获取事件自带的时间戳来作为水位线,*1000是必须要加的
                        return element.getTimeStamp() * 1000L;
                    }
                }));
        //这里标签要加个{}变成匿名内部类
        OutputTag<SensorReading> outPutLate = new OutputTag<SensorReading>("late") {
        };

        /*
         * Explain
         * 基于事件时间的开窗聚合,统计15秒内温度的最小值,这里的15秒不是机器的走15秒，而是数据中时间戳的15秒数据
         * 这里虽然按id分区,但还是窗口触发时间还是一样,就像sensor_1的窗口还会被sensor_3的id触发
         * */
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))//这里of的第二个参数是offset,默认为0
                //到了15秒后先直接输出一个结果也就是在[0,15)上在加1分钟，来了之后再聚合成一个新结果输出
                //这就说明比如[0,15)的实际关闭时间就是[0,75)，也就是水位线达到77的时候关闭
                //.allowedLateness(Time.minutes(1))
                //.sideOutputLateData(outPutLate) //若等了20秒还没来就进入侧输出流
                .maxBy("temperature");

        minTempStream.print("temperature");
        //minTempStream.getSideOutput(outPutLate).print("late");//两次迟到的数据就在这里输出
        env.execute();
    }
}
