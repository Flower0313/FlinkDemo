package com.atguigu.process;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-KeyedProcessFunction_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月26日20:53 - 周五
 * @Describe
 */
public class KeyedProcessFunction_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.keyBy(SensorReading::getId)
                .process(new MyProcess()).print();


        env.execute();
    }

    //泛型参数一是key(keyBy分组的那个key)，参数二是输入类型，参数三是输出类型
    public static class MyProcess extends KeyedProcessFunction<String, SensorReading, Integer> {

        private ValueState<Long> tsTimerState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<String,
                SensorReading, Integer>.Context ctx,
                                   Collector<Integer> out) throws Exception {
            out.collect(value.getId().length());

            //上下文
            ctx.timestamp();//时间戳
            ctx.getCurrentKey();//当前key
            //ctx.output();//里面可以传入你的侧输出流,若你涉及分流操作
            //ctx.timerService().currentProcessingTime();//获取Processing Time时间语义
            //ctx.timerService().currentWatermark();//获取Event Time时间语义
            //tsTimerState.update(ctx.timerService().currentProcessingTime());//保存当前的时间戳
            //System.out.println(ctx.timerService().currentProcessingTime());
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime());//基于当前机器时间后1秒执行
            //ctx.timerService().registerEventTimeTimer((value.getTimeStamp() + 10) * 3000L);//基于当前某个事件时间戳后10秒执行闹钟
            //ctx.timerService().deleteEventTimeTimer((value.getTimeStamp() + 10) * 2000L);//在闹钟执行10秒之后停止执行
            //ctx.timerService().deleteProcessingTimeTimer(tsTimerState.value()+10L);//这样下个时间戳进来就还可以获取到上次的时间戳

        }

        //闹钟被执行需要做的事情
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, Integer>.OnTimerContext ctx, Collector<Integer> out) throws Exception {
            System.out.println(ctx.getCurrentKey() + "闹钟" + timestamp + "响了！");
            System.out.println(ctx.timeDomain());
        }

        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }


}

