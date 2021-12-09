package com.atguigu.state;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-MapState_9
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月09日17:09 - 周四
 * @Describe
 */
public class MapState_9 {
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
                    private MapState<String, SensorReading> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        /*
                         * MapStateDescriptor(自定义名称, key的类Class, value的类Class)
                         * */
                        System.out.println("正在测试MapState状态");
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, SensorReading>("map", String.class, SensorReading.class));
                    }


                    @Override
                    public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                        //若mapState中没有相同id的则输出,实现去重的效果
                        if (!mapState.contains(value.getId())) {
                            out.collect(value);
                            //System.out.println("out.collect之后还能执行");
                            //将输出过后的id添加到状态中,以保证后面的也实现去重的功能
                            mapState.put(value.getId(), value);
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        mapState.clear();
                    }
                }).print("MapState");


        //Step-3 开始执行
        env.execute();
    }
}
