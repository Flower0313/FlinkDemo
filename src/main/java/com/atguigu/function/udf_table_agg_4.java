package com.atguigu.function;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import javax.swing.text.html.Option;
import java.util.Iterator;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @ClassName FlinkDemo-udf_table_agg_4
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日20:35 - 周二
 * @Describe 使用表聚合函数:求每组的第一名和第二名
 */
public class udf_table_agg_4 {
    public static void main(String[] args) throws Exception {
        //step-1 定义环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //step-2 读取文件
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);

        //step-3 转换为POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //step-4 将流转换为表
        Table sensorTable = tEnv.fromDataStream(dataStream, $("id"), $("timeStamp").as("ts"), $("temperature").as("temp"));
        //step-6 调用自定义表聚合函数,求出每组的前2大的值

        //sub-step-6.1 在环境中注册UDF
        tEnv.createTemporarySystemFunction("top2", Top2.class);


        //sub-step-6.2 过时的Table_API

        Table apiTable1 = sensorTable.groupBy("id")
                .flatAggregate("top2(temp) as (v, rank)")
                .select("id, v, rank");

        //sub-step-6.3 推荐的Table_API
        Table apiTable2 = sensorTable
                .groupBy($("id"))
                .flatAggregate(call("top2", $("temp")).as("v", "rk"))
                .select($("id"), $("v"), $("rk"));


        tEnv.createTemporaryView("sensor", sensorTable);


        //step-8 打印输出
        apiTable1.execute().print();
        env.execute();
    }

    //来定义AggregateFunction的实现类
    //泛型参数一就是输出的结果,参数二是Acc状态类型,Double是聚合值,Integer是聚合的元素个数
    public static class Top2 extends TableAggregateFunction<Tuple2<Double, String>, Tuple2<Double, Double>> {

        @Override
        public Tuple2<Double, Double> createAccumulator() {
            //初始化累加器
            return new Tuple2<Double, Double>(Double.MIN_VALUE, Double.MIN_VALUE);
        }

        /**
         * @param acc    已经存在的累加器
         * @param insert 从函数方法中进来的值
         */
        public void accumulate(Tuple2<Double, Double> acc, Double insert) {
            if (insert > acc.f0) {
                //若大于第一名,那就将第一名的值赋值给第二名,将新来的最大值给第一名
                acc.f1 = acc.f0;
                acc.f0 = insert;
            } else if (insert > acc.f1) {
                //若不大于第一名且大于第二名,就将值赋值给第二名
                acc.f1 = insert;
            }
        }

        public void emitValue(Tuple2<Double, Double> acc, Collector<Tuple2<Double, String>> out) {
            if (acc.f0 != Double.MIN_VALUE) {
                //输出格式为(第一大的值,名次)
                out.collect(Tuple2.of(acc.f0, "本组第一名"));
            }
            if (acc.f1 != Double.MIN_VALUE) {
                out.collect(Tuple2.of(acc.f1, "本组第二名"));
            }
        }
    }
}
