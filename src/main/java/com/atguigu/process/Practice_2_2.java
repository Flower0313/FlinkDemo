package com.atguigu.process;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-Practice_2_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月09日19:22 - 周四
 * @Describe
 */
public class Practice_2_2 {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境 & 事件时间
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimeStamp() * 1000L;
                    }
                }));

        DataStream<SensorReading> result = dataStream.keyBy(SensorReading::getId)
                .process(new KeyedProcessFunction<String, SensorReading, SensorReading>() {
                    //1.定义状态
                    private ValueState<Double> vcState;
                    private ValueState<Long> tsState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //2.注册状态
                        vcState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("vcState", Types.DOUBLE, 0.0D));
                        tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsState", Types.LONG));
                    }

                    @Override
                    public void close() throws Exception {
                        vcState.clear();
                        tsState.clear();
                    }

                    @Override
                    public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                        System.out.println("当前时间戳:" + ctx.timestamp());
                        if (value.getTemperature() > vcState.value() && tsState.value() == null) {
                            System.out.println("温度上升!");
                            Long timestamp = ctx.timestamp();
                            //注册5秒的定时器,假设1秒注册的定时器,那么5秒后触发,相当于在6秒后就是在7秒执行
                            ctx.timerService().registerEventTimeTimer(timestamp + 5000L);
                            //将当前时间戳存入时间戳状态
                            tsState.update(timestamp + 5000L);
                        } else {
                            //若温度下降,且时间戳不为空,就删除定时器
                            if (value.getTemperature() < vcState.value() && tsState != null) {
                                System.out.println("温度下降!");
                                ctx.timerService().deleteEventTimeTimer(tsState.value());
                                tsState.clear();
                            }
                        }
                        //最后无论怎么样都要更新上一个温度状态值
                        vcState.update(value.getTemperature());
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, SensorReading>.OnTimerContext ctx, Collector<SensorReading> out) throws Exception {
                        System.out.println("上升警告!");
                        tsState.clear();
                    }
                });
        result.print();

        env.execute();
    }
}
