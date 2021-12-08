package com.atguigu.trans;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-trans_map_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日0:09 - 周日
 * @Describe
 */
public class trans_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//并行度1，只开放一个slot

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> source = env.readTextFile(inputPath);

        //TODO 1.1 map，把读入的String转成长度输出,消费一个元素产生一个元素,DStream->DStream
        DataStream<Integer> mapStream = source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                //返回一行字符串的长度
                return value.length();
            }
        });

        //1.2 将map使用lambda的方式value->value.length 可以变成_.length,但是在java中要这样String::length
        DataStream<Integer> mapStream2 = source.map(String::length);

        //TODO 2.1 flatmap,按逗号切分字段,消费一个元素产生多个元素,DStream->DStream
        //FlatMapFunction<T,O>,T就是输入元素,O就是out
        DataStream<String> flatMapStream = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] line = value.split(",");
                for (String s : line) {
                    out.collect(s);
                }
            }
        });

        //2.2 使用lambda方式来运行
        DataStream<String> flatMapStream2 = source.flatMap((value, out) -> {
            for (String s : value.split(",")) {
                out.collect(s);
            }
        });


        //TODO 3.1 filter，筛选sensor_1开头的数据,FilterFunction接口只有唯一的方法,可以使用lambda表达式
        //DStream->DStream
        DataStream<String> filterStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("sensor_1");
            }
        });

        //3.2 将匿名实现类改写为lambda表达式
        DataStream<String> filterStream2 = source.filter((value) -> value.startsWith("sensor_1"));


        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");
        env.execute();

    }
}

