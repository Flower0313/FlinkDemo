package com.atguigu.state;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-ReducingState_7
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月09日15:55 - 周四
 * @Describe
 */
public class ReducingState_7 {
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境 & 数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //Step-2 实现自己的逻辑
        dataStream.keyBy(SensorReading::getId)
                .process(new KeyedProcessFunction<String, SensorReading, SensorReading>() {
                    //定义聚合状态
                    private ReducingState<SensorReading> reducingState;

                    /*
                     * Attention
                     * 这里是注册方式和之前的状态都不一样,中间的参数需要实现一个聚合参数的逻辑
                     * */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("reducing", new ReduceFunction<SensorReading>() {
                            @Override
                            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                                return new SensorReading(value1.getId(), value2.getTimeStamp(), value1.getTemperature() + value2.getTemperature());
                            }
                        }, SensorReading.class));
                    }

                    @Override
                    public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                        /*
                        * Conclusion - 执行逻辑
                        * 1.每条数据进来后都会相同key的进行聚合,而聚合的逻辑就定义在状态中,processElement中只是定义了一下
                        *   存数据取数据的过程而已。
                        * 2.每个分区的reducingState中有且仅保留一个聚合值,相当于聚合版的ValueState
                        * 3.AggregatingState<IN, OUT>,而ReducingState<T>,所以前者能返回不一样的类型
                        * */
                        reducingState.add(value);//当数据添加进去时就触发了聚合操作

                        SensorReading sensorReading = reducingState.get();

                        out.collect(sensorReading);
                    }

                    @Override
                    public void close() throws Exception {
                        reducingState.clear();
                    }
                }).print("ReducingState");
        env.execute();
    }
}
