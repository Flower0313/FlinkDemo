package com.atguigu.trans;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-trans_union_9
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月04日20:24 - 周六
 * @Describe union
 */
public class trans_union_9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//并行度1，只开放一个slot
        /*
         * 1.可以超过2个或2个以上的流进行union,产生一个包含所有流的新流
         * 2.多个流的类型必须一样
         * */
        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> stream2 = env.fromElements(10, 20, 30, 40, 50);
        DataStreamSource<Integer> stream3 = env.fromElements(100, 200, 300, 400, 500);

        // 把多个流union在一起成为一个流, 这些流中存储的数据类型必须一样: 水乳交融
        stream1.union(stream2)
                .union(stream3)
                .print();


        env.execute();
    }
}
