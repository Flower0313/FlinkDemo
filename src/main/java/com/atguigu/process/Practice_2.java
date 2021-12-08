package com.atguigu.process;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-Practice_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月26日23:07 - 周五
 * @Describe 监控温度传感器的温度值，如果在10秒内连续上升就报警
 */
public class Practice_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.keyBy(SensorReading::getId)
                .process(new TempConsIncrWarning(5)).print();


        env.execute();
    }

    //第一个参数是id类型，第二个参数输入类型，第三个参数输出类型
    public static class TempConsIncrWarning extends KeyedProcessFunction<String, SensorReading, Tuple2<String, String>> {
        private Integer interval;//时间间隔

        private ValueState<Double> lastTempState;//上次温度状态
        private ValueState<Long> timerTsState;//上次时间戳

        @Override//获取状态句柄
        public void open(Configuration parameters) throws Exception {
            //Attention flink不推荐在这里使用初始值
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class, 0.0));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
        }


        public TempConsIncrWarning(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            //step-1.取出上次状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            //step-2.如果温度上升且没有定时器，就注册10秒的定时器,开始等待
            if (value.getTemperature() > lastTemp && timerTs == null) {
                //开始时间就是上次的时间戳，因为是从这次对比才知道上次相较于这次是在上升
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;//需要等待到多久
                ctx.timerService().registerProcessingTimeTimer(ts);
                timerTsState.update(ts);
            } else if (value.getTemperature() < lastTemp && timerTs != null) {//如果温度下降，删除之前的定时器
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }


            //更新温度状态
            lastTempState.update(value.getTemperature());
        }

        @Override//时间到了时间窗口执行的任务
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, Tuple2<String, String>>.OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            System.out.println("传感器" + ctx.getCurrentKey() + "温度报警");
            timerTsState.clear();
        }
    }
}
