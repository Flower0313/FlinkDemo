package com.atguigu.practice.pv;

import bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-flink_pv_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日1:02 - 周三
 * @Describe
 */
public class flink_pv_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
         * Attention
         * 注意使用匿名实现方式并且使用了泛型，返回的数据类型需要使用returns(Types.XX)来明确告诉flink返回来,
         * 但使用方法的时候就不需要指定returns,
         * 这里我们返回的Tuple2<String,Long>就使用了泛型,所以要指定returns(Types.XX)
         * */
        env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] split = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4])
                    );
                }).filter(behavior -> "pv".equals(behavior.getBehavior()))
                .map(behavior -> {
                    return Tuple2.of("pv", 1L);
                }).returns(Types.TUPLE(Types.STRING, Types.LONG)).print("test");


        env.execute();
    }
}
