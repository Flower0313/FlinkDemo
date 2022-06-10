package com.holden.function;

import com.holden.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import java.util.Iterator;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @ClassName FlinkDemo-udf_agg_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日19:54 - 周二
 * @Describe 聚合函数:求每组传感器的平均值
 */
public class udf_agg_3 {
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

        //step-6 调用自定义函数,求当前传感器的平均温度值
        //sub-step-6.1 在环境中注册UDF
        tEnv.createTemporarySystemFunction("avgTemp", AvgTemp.class);

        //sub-step-6.2 过时的Table API
        Table apiTable1 = sensorTable.groupBy("id")
                .aggregate("avgTemp(temp) as avgtmp")
                .select("id,avgtmp");

        //sub-step-6.2 推荐的Table API
        Table apiTable2 = sensorTable.groupBy($("id"))
                .aggregate(call("avgTemp", $("temp")).as("avgtmp"))
                .select($("id"), $("avgtmp"));

        //sub-step-6.3 SQL
        tEnv.createTemporaryView("sensor", sensorTable);
        Table sqlTable1 = tEnv.sqlQuery(
                "select id,avgTemp(temp)" +
                        "from sensor group by id");


        //step-8 打印输出
        //sqlTable1.execute().print();

        /*
         * attention
         *  这里就需要用到撤回流了,因为在输出的流中会撤回之前的值再将新值插入
         * */
        tEnv.toRetractStream(sqlTable1, Row.class).print();

        env.execute();
    }

    //来定义AggregateFunction的实现类
    //泛型参数一就是输出的结果,参数二是Acc状态类型,Double是聚合值,Integer是聚合的元素个数
    public static class AvgTemp extends AggregateFunction<Double, Tuple2<Double, Integer>> {

        //返回最终的聚合结果
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        //创建当前的累加器
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<Double, Integer>(0.0, 0);
        }

        /**
         * 每一行数据进来都会调用此方法来更新accumulator,当所有数据都处理完毕,
         * 通过调用getValue()方法来计算和返回最终结果。
         *
         * @param accumulator 中间聚合值
         * @param temp        进来的数据
         */
        public void accumulate(Tuple2<Double, Integer> accumulator, Double temp) {
            //聚合值
            accumulator.f0 += temp;
            //个数
            accumulator.f1 += 1;
        }

        /**
         * 将不同的累加器合并
         *
         * @param acc 当前的累加器
         * @param it  之前的所有累加器
         */
        public void merge(Tuple2<Double, Integer> acc, Iterable<Tuple2<Double, Integer>> it) {
            Iterator<Tuple2<Double, Integer>> iter = it.iterator();
            while (iter.hasNext()) {
                Tuple2<Double, Integer> next = iter.next();
                acc.f0 += next.f0;
                acc.f1 += next.f1;
            }
        }

        /**
         * 重置累加器
         *
         * @param acc 累加器
         */
        public void resetAccumulator(Tuple2<Double, Integer> acc) {
            acc.f0 = 0D;
            acc.f1 = 0;
        }
    }
}
