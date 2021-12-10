package com.atguigu.trans;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-trans_keyBy_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日10:31 - 周日
 * @Describe 聚合操作
 * 1.把流中的数据分到不同的分区中,具有相同key的元素会分到同一个分区中
 * 2.一个分区中可以有多重不同的key
 * 3.内部是使用的hash分区来实现的
 * 4.没有实现hashCode()的JavaBean类不能使用keyBy,因为这样的分组没有意义
 * 5.数组也不能使用keyBy
 * 6.DataStream -> KeyedStream
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

        //dataStream.print("origin");

        //根据id分组
        //dataStream.keyBy("id"); //方式一：已过期
        //dataStream.keyBy(0); //方式二：已过期
        //dataStream.keyBy(x->x.getId()); //方式三

        //方式四:使用匿名内部类
        KeyedStream<SensorReading, Object> keyedStream = dataStream.keyBy(new KeySelector<SensorReading, Object>() {

            @Override
            public Object getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        });

        //方式五：方式四的lambda化,keyBy设置不setParallelism，因为它本身就是会根据hash来分区
        KeyedStream<SensorReading, String> kyStream = dataStream.keyBy(SensorReading::getId);//相当于scala中的_.getId

        //求组内温度最大值，但这个求最大值不是直接求，而是滚动求，就和sql中窗口函数的order by作用域滚动一样
        //组内来一条数据就比一次，每组各自统计各自的
        //DataStream<SensorReading> resultStream = kyStream.maxBy("temperature");

        env.execute();
    }
}

















