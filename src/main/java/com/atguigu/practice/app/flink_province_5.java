package com.atguigu.practice.app;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.common.CommonEnv.PROVINCE_SOURCE;
import static org.apache.flink.api.common.typeinfo.Types.*;

/**
 * @ClassName FlinkDemo-flink_province_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日14:07 - 周三
 * @Describe 各省份页面广告点击量实时统计
 */
public class flink_province_5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.readTextFile(PROVINCE_SOURCE)
                .map(line -> {
                    String[] l = line.split(",");
                    return new AdsClickLog(
                            Long.valueOf(l[0]),
                            Long.valueOf(l[1]),
                            l[2],
                            l[3],
                            Long.valueOf(l[4])
                    );
                }).map(log -> Tuple2.of(Tuple2.of(log.getProvince(), log.getAdId()), 1L))
                .returns(TUPLE(TUPLE(STRING, LONG), LONG))//声明泛型的类型
                .keyBy(new KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> getKey(Tuple2<Tuple2<String, Long>, Long> value) throws Exception {
                        //指定key
                        return value.f0;
                    }
                }).sum(1)
                .print("((省份,广告),总数)");
        env.execute();
    }
}
