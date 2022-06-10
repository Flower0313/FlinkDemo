package com.holden.state;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-AggregatingState_8
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月09日16:21 - 周四
 * @Describe
 */
public class AggregatingState_8 {
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
                .process(new KeyedProcessFunction<String, SensorReading, Tuple2<String, Double>>() {
                    //AggregatingState<IN, OUT>
                    private AggregatingState<Tuple2<String, Double>, Tuple2<String, Double>> avgState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        /*
                         * Explain
                         * AggregatingStateDescriptor<IN,ACC,OUT>
                         *
                         * AggregatingStateDescriptor(name,AggregateFunction<IN, ACC, OUT>,Class<ACC> 累加器类型)
                         * */
                        avgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Tuple2<String, Double>, Tuple3<String, Double, Integer>, Tuple2<String, Double>>("avg", new AggregateFunction<Tuple2<String, Double>, Tuple3<String, Double, Integer>, Tuple2<String, Double>>() {
                            //初始化累加器,(Id,聚合值,数据个数)
                            @Override
                            public Tuple3<String, Double, Integer> createAccumulator() {
                                return Tuple3.of("", 0.0D, 0);
                            }

                            //累加逻辑,add(IN,ACC)
                            @Override
                            public Tuple3<String, Double, Integer> add(Tuple2<String, Double> value, Tuple3<String, Double, Integer> accumulator) {
                                return Tuple3.of(value.f0, accumulator.f1 + value.f1, accumulator.f2 + 1);
                            }

                            //对最后的聚合值做逻辑处理,这里是求平均值
                            @Override
                            public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> accumulator) {
                                return Tuple2.of(accumulator.f0, accumulator.f1 / accumulator.f2);
                            }

                            //聚合两个累加器
                            @Override
                            public Tuple3<String, Double, Integer> merge(Tuple3<String, Double, Integer> a, Tuple3<String, Double, Integer> b) {
                                return Tuple3.of(a.f0, a.f1 + b.f1, a.f2 + b.f2);
                            }
                        }, Types.TUPLE(Types.STRING, Types.DOUBLE, Types.INT)));
                    }


                    @Override
                    public void processElement(SensorReading value, KeyedProcessFunction<String, SensorReading, Tuple2<String, Double>>.Context ctx, Collector<Tuple2<String, Double>> out) throws Exception {
                        //将当前数据累加进状态
                        avgState.add(Tuple2.of(value.getId(), value.getTemperature()));

                        //取出状态中的数据
                        out.collect(avgState.get());
                    }

                    @Override
                    public void close() throws Exception {
                        avgState.clear();
                    }
                }).print("AggregatingState");


        env.execute();
    }
}
