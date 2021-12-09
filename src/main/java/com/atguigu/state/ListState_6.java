package com.atguigu.state;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @ClassName FlinkDemo-ListState_6
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月09日15:06 - 周四
 * @Describe 列表ListState状态:输出每个传感器最高的3个水位值
 */
public class ListState_6 {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境 & 数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        //Step-2 按key分区走自己的逻辑
        dataStream.keyBy(SensorReading::getId)
                .process(new KeyedProcessFunction<String, SensorReading, Tuple2<String, List<Double>>>() {
                    private ListState<Double> vsState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("---- 向注册状态 ----");
                        //在生命周期准备好时注册状态
                        vsState = getRuntimeContext().getListState(new ListStateDescriptor<Double>("vs", Double.class));
                    }

                    @Override
                    public void close() throws Exception {
                        //清空状态中的数据
                        vsState.clear();
                    }

                    @Override//每条数据进来都会调用这个方法,注意这是流式数据,每条值都是最后的聚合值
                    public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, Tuple2<String, List<Double>>>.Context ctx, Collector<Tuple2<String, List<Double>>> out) throws Exception {
                        //1.将进来的数据温度值加入进去
                        vsState.add(value.getTemperature());
                        //2.比较大小,取出前3大的数据,每条数据进来都会输出截止当前为止最大的3个数
                        List<Double> vcs = new ArrayList<>();
                        for (Double aDouble : vsState.get()) {
                            vcs.add(aDouble);
                        }
                        //3.排序,倒序
                        vcs.sort((o1, o2) -> -o1.compareTo(o2));

                        //4.当长度超过3的时候移除最后一个,只在状态中保留3个最大数,多余值删除
                        if (vcs.size() > 3) {
                            vcs.remove(3);
                        }
                        //5.将当前最大的三个值更新上去
                        vsState.update(vcs);
                        out.collect(Tuple2.of(value.getId(), vcs));
                    }
                }).print("ListState");


        env.execute();
    }
}
