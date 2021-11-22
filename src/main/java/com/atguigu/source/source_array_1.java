package com.atguigu.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @ClassName FlinkDemo-source_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月20日17:59 - 周六
 * @Describe 从集合中读取
 */
public class source_array_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//设置这个后执行顺序一致，因为只有一个并行度

        // 1.Source:从集合读取数据
        DataStream<SensorReading> source1 = env.fromCollection(
                //转换为对象数组
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_6", 1547718201L, 15.4),
                        new SensorReading("sensor_7", 1547718202L, 6.7),
                        new SensorReading("sensor_10", 1547718205L, 38.1)
                )
        );

        DataStream<Integer> source2 = env.fromElements(1, 2, 13, 313, 189);

        /*
        * source1和source2一点关系都没有，但他们都会一起抢占slot资源
        * 所以source1和source2的顺序不确定，但是他们内部的顺序是确定的
        * */

        // 2.打印
        source1.print("data");//data是流的名称
        source2.print("int");

        // 3.执行
        env.execute("job1");//job1是jobName

    }
}


