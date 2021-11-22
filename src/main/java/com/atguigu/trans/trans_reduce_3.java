package com.atguigu.trans;

import com.atguigu.source.SensorReading;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-trans_reduce_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日12:48 - 周日
 * @Describe
 */
public class trans_reduce_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //根据id分组
        KeyedStream<SensorReading, String> kyStream = dataStream.keyBy(SensorReading::getId);//相当于scala中的_.getId

        //reduce聚合，取最大温度值和当前最新时间戳
        DataStream<SensorReading> reduce = kyStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                //第一个参数：因为已经按id分组了，所以这里填写value1或value2的id都一样
                //第二个参数：因为要是最新的时间戳所以要填写value2的，因为每次reduce完的结果都填充在value1，新数据从value2进来
                //第三个参数：比较逻辑
                return new SensorReading(value1.getId(), value2.getTimeStamp(), Math.max(value1.getTemperature(), value2.getTemperature()));
            }
        });

        //lambda版本
        /*kyStream.reduce((curState, newData) -> {
            return new SensorReading(curState.getId(), newData.getTimeStamp(), Math.max(curState.getTemperature(), newData.getTemperature()));
        });*/

        reduce.print();
        env.execute();
    }
}
