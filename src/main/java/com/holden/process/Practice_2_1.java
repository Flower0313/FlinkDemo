package com.holden.process;

import com.holden.bean.SensorReading;
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
 * @Describe 监控温度传感器的温度值，如果在5秒内连续上升就报警
 */
public class Practice_2_1 {
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
            //step-1 取出上次状态
            Double lastTemp = lastTempState.value();
            Long timerTs = timerTsState.value();

            //step-2.1 如果温度上升且没有定时器闹钟，就注册5秒的定时器,开始等待
            if (value.getTemperature() > lastTemp && timerTs == null) {
                //声明定时器,处理事件定时器执行开始时间就是当前时间+5秒
                long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;//需要等待到多久
                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(ts);
                //将启动定时器的时间戳保存在状态中,方便后面的状态来取消这个定时器
                timerTsState.update(ts);
            }
            //step-2.2 若温度下降且时间戳不为空,就删除定时器,若暂停失败就证明超过5秒了,若暂停成功就说明还未达到5秒温度就下降了
            else if (value.getTemperature() < lastTemp && timerTs != null) {
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                timerTsState.clear();
            }
            //step-2.3 若温度下降且时间戳为空,啥都不执行

            //step-2.4 若温度一直连续上升,那么5秒后会调用onTimer方法,这个方法调用后会清空timerTsState,又能开一个5秒窗口了


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
