package com.atguigu.window;

import com.atguigu.source.SensorReading;
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
public class eventtime_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //env.getConfig().setAutoWatermarkInterval(50L);//默认200毫秒

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                //乱序使用forBoundedOutOfOrderness，顺序使用forMonotonousTimestamps
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))//延迟时间
                //x就是SensorReading类型的,y就是Long类型的recordTimestamp,以毫秒为单位,所以要*1000
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {//这里将自定义SensorReading类型传入进去
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        //获取事件自带的时间戳来作为水位线,*1000是必须要加的
                        return element.getTimeStamp() * 1000L;
                    }
                }));
        //这里标签要加个{}变成匿名内部类，这样就会变成eventtime_3$1内部类，这样周期就和这个程序一样长
        OutputTag<SensorReading> outPutLate = new OutputTag<SensorReading>("late"){};

        //基于事件时间的开窗聚合,统计15秒内温度的最小值,这里的15秒不是机器的走15秒，而是数据中时间戳的15秒数据
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))//这里of的第二个参数是offset,默认为0
                //到了15秒后先直接输出一个结果也就是在[0,15)上在加1分钟，来了之后再聚合成一个新结果输出
                //这就说明比如[0,15)的实际关闭时间就是[0,75)，也就是水位线达到77的时候关闭
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outPutLate) //若等了20秒还没来就进入侧输出流
                .minBy("temperature");

        minTempStream.print("temperature");
        minTempStream.getSideOutput(outPutLate).print("late");//两次迟到的数据就在这里输出
        env.execute();
    }
}
