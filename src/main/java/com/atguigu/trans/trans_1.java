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

        //1.map，把读入的String转成长度输出
        DataStream<Integer> mapStream = source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                //返回一行字符串的长度
                return value.length();
            }
        });

        //2.flatmap,按逗号切分字段
        DataStream<String> flatMapStream = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] line = value.split(",");
                for (String s : line) {
                    out.collect(s);
                }
            }
        });


        //3.filter，筛选sensor_1开头的数据
        DataStream<String> filterStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("sensor_1");
            }
        });




        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        env.execute();

    }
}

