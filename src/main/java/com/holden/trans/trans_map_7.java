package com.holden.trans;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * @ClassName FlinkDemo-trans_map_6
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月26日17:50 - 周五
 * @Describe 消费一个元素产生一个元素
 */
public class trans_map_7 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.map(new RichMapFunction<SensorReading, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("只执行一次");
            }

            @Override
            public String map(SensorReading value) throws Exception {
                return value.toString();
            }
        }).print("同步1>>>");
        //dataStream.print("同步>>>");//默认rebalance轮询分区

        dataStream.map(new RichMapFunction<SensorReading, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("只执行一次");
            }

            @Override
            public String map(SensorReading value) throws Exception {
                return value.toString();
            }
        }).print("同步2>>>");

        /*DataStream<String> resultStream = AsyncDataStream.unorderedWait(inputStream,
                new MapAsyncFunction(),
                1000,
                TimeUnit.SECONDS,
                1000);

        resultStream.print("异步>>>");*/


        //dataStream.keyBy(SensorReading::getId).print("keyBy");//按hash分区

        env.execute();
    }

    /**
     * 同步函数，先处理第一条，第二条数据等待第一条数据处理完成后再进行处理
     */
    public static class MyRichFunction extends RichMapFunction<Integer, Integer> {

        public void open() {
            System.out.println("open...执行一次");
        }

        @Override
        public Integer map(Integer integer) throws Exception {
            return null;
        }

        public void close() {
            System.out.println("close...执行一次");
        }
    }

    /**
     * 异步方法调用
     */
    public static class MapAsyncFunction extends RichAsyncFunction<String, String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("初始化....");
        }

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
            CompletableFuture.supplyAsync(new Supplier<String>() {
                @Override
                public String get() {
                    return input;
                }
            }).thenAccept(x -> {
                resultFuture.complete(Collections.singleton(input));
            });

        }

        @Override
        public void timeout(String input, ResultFuture<String> resultFuture) throws Exception {
            super.timeout(input, resultFuture);
        }
    }
}













