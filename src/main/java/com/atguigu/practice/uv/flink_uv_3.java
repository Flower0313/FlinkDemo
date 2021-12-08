package com.atguigu.practice.uv;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

import static com.atguigu.common.CommonEnv.PV_SOURCE;

/**
 * @ClassName FlinkDemo-flink_uv_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日9:27 - 周三
 * @Describe 需求,统计每个用户的访问次数
 */
public class flink_uv_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.readTextFile(PV_SOURCE)
                .flatMap((String value, Collector<Tuple2<String, Long>> out) -> {
                    String[] split = value.split(",");

                    UserBehavior ub = new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4])
                    );
                    if ("pv".equals(ub.getBehavior())) {
                        out.collect(Tuple2.of("uv", ub.getUserId()));
                    }

                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value -> value.f1)
                .process(new KeyedProcessFunction<Long, Tuple2<String, Long>, Tuple2<Long, Integer>>() {
                    //todo 自定义数量状态,每个keyBy的状态都是分区独有的,根据结果就可以看出
                    private ValueState<Integer> cnt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        cnt = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("cnt", Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, KeyedProcessFunction<Long, Tuple2<String, Long>, Tuple2<Long, Integer>>.Context ctx, Collector<Tuple2<Long, Integer>> out) throws Exception {
                        //防止第一次进来状态值为空,所以要加一个判断赋初值
                        if (cnt.value() == null) {
                            cnt.update(1);
                        } else {
                            //为后面的值直接累加
                            cnt.update(cnt.value() + 1);
                        }

                        //输出统计好的值,请注意这里流式数据,所以每一次进来的值都是结果值,生生不息...
                        out.collect(new Tuple2<Long, Integer>(value.f1, cnt.value()));
                    }
                }).print();

        env.execute();
    }
}
