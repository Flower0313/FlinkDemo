package com.atguigu.trans;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-trans_10
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月04日20:40 - 周六
 * @Describe 滚动聚合
 *  1.来一条聚合计算一条
 *  2.这些算子必须在keyBy之后使用,因为他们都属于keyedStream中
 *  3.聚合算子作用范围都在分组内的,也就是不同组分开算
 *  4.max和maxBy的区别:
 *      max取指定字段的当前的最大值,如果有多个字段,其他非比较字段,以第一条为准
 *      maxBy取指定字段的当前的最大值,如果有多个字段,其他字段以最大值那条数据为准
 */
public class trans_max_10 {
    /*DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5);
        KeyedStream<Integer, String> kbStream = stream.keyBy(ele -> ele % 2 == 0 ? "奇数" : "偶数");
        kbStream.sum(0).print("sum");//偶数组和奇数组分别相加

        kbStream.max(0).print("max");
        kbStream.min(0).print("min");*/

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//并行度1，只开放一个slot



        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);

        //keyedStream.sum("temperature").print("sum");
        //keyedStream.max("temperature").print("max");
        keyedStream.maxBy("temperature",true).print("maxBy");

        env.execute();
    }
}
