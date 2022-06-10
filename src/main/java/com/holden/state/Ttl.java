package com.holden.state;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-Ttl
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月16日11:50 - 周四
 * @Describe 数据状态清理
 */
public class Ttl {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.socketTextStream("hadoop102", 31313);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);

        DataStream<String> map = keyedStream.map(new RichMapFunction<SensorReading, String>() {
            private ValueState<String> dataState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //定义状态分发器
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("flower", String.class);

                //定义状态有效期
                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.milliseconds(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                //给状态分发器注册有效期
                stateDescriptor.enableTimeToLive(stateTtlConfig);
                //注册状态
                dataState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public String map(SensorReading value) throws Exception {
                if (dataState.value() == null) {
                    dataState.update(value.toString());
                }
                System.out.println("上一次的状态值为:" + dataState.value() + " || 当前值:" + value.toString());
                return value.toString();
            }
        });
        /*
         * Example-1
         * 设置状态清空策略为Time.days(1),即一天清空状态值一次
         * 第一次输入s1,1,1
         * 第二次输入s1,2,2
         *
         * Result-1
         * null || s1,1,1
         * s1,1,1 || s1,2,2
         *
         * Example-2
         * 设置状态清空策略为Time.milliseconds(1),即一毫秒情况状态值一次
         * 第一次输入s1,1,1
         * 第二次输入s1,2,2
         *
         * Result-2
         * null || s1,1,1
         * null || s1,2,2
         *
         * 可以看到状态在1ms内立即清空了
         * */

        map.print();

        env.execute();
    }
}
