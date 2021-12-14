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
 * @ClassName FlinkDemo-wm_punctuated
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月11日9:24 - 周六
 * @Describe
 */
public class wm_punctuated_2 {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境 & 数据
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000L);
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
                return new MyPunctuatedWm(2000L);//延迟时间
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
        env.execute();
    }

    public static class MyPunctuatedWm implements WatermarkGenerator<SensorReading> {
        //最大时间戳
        private Long maxTs;
        //最大延迟时间
        private Long maxDelay;

        public MyPunctuatedWm(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + this.maxDelay + 1;
        }


        @Override
        public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
            //获取当前数据中最大的时间戳并赋值
            System.out.println("生成Watermark");
            maxTs = Math.max(eventTimestamp, maxTs);
            output.emitWatermark(new Watermark(maxTs - maxDelay - 1));

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}
