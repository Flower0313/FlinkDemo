package com.atguigu.trans;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-trans_keyBy_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日10:31 - 周日
 * @Describe 聚合操作
 */
public class trans_keyBy_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //DataStream不是完成聚合操作，需要KeyedStream来做
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //根据id分组
        //dataStream.keyBy("id"); //方式一：已过期
        //dataStream.keyBy(0); //方式二：已过期
        //dataStream.keyBy(x->x.getId()); //方式三

        //方式四：方式三的简写
        KeyedStream<SensorReading, String> kyStream = dataStream.keyBy(SensorReading::getId);//相当于scala中的_.getId

        //求组内温度最大值，但这个求最大值不是直接求，而是滚动求，就和sql中窗口函数的order by作用域滚动一样
        //组内来一条数据就比一次，每组各自统计各自的
        DataStream<SensorReading> resultStream = kyStream.maxBy("temperature");

        resultStream.print();

        env.execute();
    }
}
