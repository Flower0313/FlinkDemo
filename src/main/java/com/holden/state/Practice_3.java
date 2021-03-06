package com.holden.state;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-Practice_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月25日23:05 - 周四
 * @Describe 利用状态管理实现报警信息需求，若连续的两个温度差超过10读就报警
 */
public class Practice_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple3<String, Double, Double>> warningStream =
                dataStream.keyBy(SensorReading::getId).flatMap(new TempIncreaseWarning(10.0));

        warningStream.print();

        env.execute();
    }

    /**
     * todo SensorReading是输入类型,Tuple3是输出类型
     * 每个分区都有自己的状态值,也就是按keyBy的分组
     */
    public static class TempIncreaseWarning extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
        //温度跳变阈值
        private final Double threshold;

        //温度跳变阈值
        public TempIncreaseWarning(Double threshold) {
            this.threshold = threshold;
        }

        //定义单值ValueState状态,保存上一次的温度值
        private ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取当前上下文,注册ValueState状态分发器,因为在open后面才打开生命周期就能获得到getRuntimeContext
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
        }


        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
            //step-1 当有新值进来时先获取上次的单值状态
            Double lastTemp = lastTempState.value();

            if (lastTemp != null) {
                //step-2 计算当前值与上个值的差值
                double diff = Math.abs(value.getTemperature() - lastTemp);//计算这次和上次状态的差值
                if (diff >= threshold) {//超过阈值就输出
                    out.collect(new Tuple3<>(value.getId(), lastTemp, value.getTemperature()));
                }
            }
            //step-3 不管是大于还是小于都会将当前值存入状态,作为下一次的前一次状态值
            lastTempState.update(value.getTemperature());
        }

        @Override
        public void close() throws Exception {
            lastTempState.clear();
        }
    }
}
