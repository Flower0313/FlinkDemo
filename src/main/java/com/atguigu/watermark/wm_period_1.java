package com.atguigu.watermark;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ClassName FlinkDemo-wm_period_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月11日8:42 - 周六
 * @Describe 周期性watermark
 */
public class wm_period_1 {
    public static void main(String[] args) {
        //Step-1 准备环境 & 数据
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //这里需要用socket流来读取数据，因为如果读文件数据，没有15秒程序就结束了，窗口也消失了
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //Step-3 自定义WaterMark
        SingleOutputStreamOperator<SensorReading> watermarks = dataStream.assignTimestampsAndWatermarks(new WatermarkStrategy<SensorReading>() {
            //生成自定义的watermark
            @Override
            public WatermarkGenerator<SensorReading> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPeriodWm(2000L);//延迟时间
            }
        }//抽取时间戳作为水位线
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {//提取数据的时间戳
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimeStamp() * 1000L;
                    }
                }));

        watermarks.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("temperature")
                .print();
    }


    //Step-2 自定义周期性水位线生成方法
    public static class MyPeriodWm implements WatermarkGenerator<SensorReading> {

        //最大时间戳
        private Long maxTs;
        //最大延迟时间
        private Long maxDelay;

        public MyPeriodWm(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + this.maxDelay + 1;
        }

        @Override//当数据来的时候调用
        public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(eventTimestamp, maxTs);
        }

        @Override//周期性调用
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("生成WaterMark");
            //为了使水位线也实现左闭右开[x,y)
            output.emitWatermark(new Watermark(maxTs - maxDelay - 1L));
        }
    }
}
