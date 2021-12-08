package com.atguigu.practice.pv;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import static com.atguigu.common.CommonEnv.PV_SOURCE;


/**
 * @ClassName FlinkDemo-flink_pv_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日8:57 - 周三
 * @Describe pv实现方式二
 */
public class flink_pv_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.readTextFile(PV_SOURCE)
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
                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, String>() {
                    //定义局部变量,仅存在内存
                    long count = 0;

                    @Override
                    public void processElement(UserBehavior value, KeyedProcessFunction<String, UserBehavior, String>.Context ctx, Collector<String> out) throws Exception {
                        count++;
                        out.collect("当前pv数:" + count);
                    }
                }).print();
        env.execute();
    }
}
