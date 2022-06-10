package com.holden.watermark;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-wm_period_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月11日8:42 - 周六
 * @Describe 周期性watermark
 */
public class wm_period_1 {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境 & 数据
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(3000L);
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
                return new MyPeriodWm(0L);//延迟时间
            }
        }//抽取时间戳作为水位线
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {//提取数据的时间戳
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        //只有数据来的时候才会调用此方法抽取时间戳给水位线,不然水位线一直是默认值
                        System.out.println("抽取时间戳");
                        return element.getTimeStamp() * 1000L;
                    }
                }));


        watermarks.process(new ProcessFunction<SensorReading, String>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
                ctx.timerService().currentWatermark();
                /* Q&A!
                 * Q1:为什么水位线是上一个时间戳对应的水位线?
                 * A1:因为它的执行顺序在当前时间戳的水位线生成前,所以下一个水位还没执行前,processElement就执行了,
                 *    故输出的是上一个时间戳对应的水位线
                 *
                 * */
                System.out.println("当前水位线:" + ctx.timerService().currentWatermark());
                System.out.println("当前时间戳:" + ctx.timestamp());
                System.out.println("------------------------------------------");
            }
        }).print();
        /*
        * Result!!
        *  当有一条数据进入时,上述方法执行顺序:
        *  1.extractTimestamp -- 抽取数据的时间戳
        *  2.processElement -- 所以此时打印的还是上一个水印,因为还没经过onEvent方法更新
        *  3.onEvent -- 给当前水位线赋新值,方便水位线的更换
        *  4.onPeriodicEmit(从头到尾周期性执行)
        * */

        env.execute();
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

        @Override//数据性调用,每条数据进来后都会调用
        public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("onEvent");
            maxTs = Math.max(eventTimestamp, maxTs);
        }

        @Override//周期性调用
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("生成WaterMark" + (maxTs - maxDelay - 1L));
            //为了使水位线也实现左闭右开[x,y)
            output.emitWatermark(new Watermark(maxTs - maxDelay - 1L));
        }
    }
}
