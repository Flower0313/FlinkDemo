package com.holden.test;

import com.holden.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Timer;
import java.util.TimerTask;

/**
 * @ClassName FlinkDemo-TimerDemo
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月27日20:56 - 周一
 * @Describe
 */
public class TimerDemo {
    public static void main(String[] args) throws Exception {
        //Step-1 配置环境 & 数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        //step-2 逻辑操作
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("no-match") {
        };

        SingleOutputStreamOperator<String> process = dataStream.process(new MyProcessFunction());

        //Step-3 输出
        process.print("正常数据>>>>>>");
        process.getSideOutput(outputTag).print("未匹配上数据>>>>>>");


        //step-4 环境执行
        env.execute();
    }

    public static class MyProcessFunction extends ProcessFunction<SensorReading, String> {

        //自定义TimerTask内部类
        public class myTask extends TimerTask {
            private ProcessFunction<SensorReading, String>.Context ctx;
            private SensorReading value;

            private OutputTag<SensorReading> noMatch = new OutputTag<SensorReading>("no-match") {
            };

            public myTask(ProcessFunction<SensorReading, String>.Context ctx, SensorReading value) {
                this.ctx = ctx;
                this.value = value;
            }

            @Override
            public void run() {
                /*
                 * Prepare
                 * 在这里写一个while()读取广播流key的循环,直到读取到对应的key数据,就进行分流输出
                 * 但这里首先做一个ctx侧输出流的测试,因为在这里处理的数据也要分别写出到kafka和hbase中
                 *
                 * Q&A
                 * Q1:为什么这里这timer、ctx、outputTag和value都不为null,会报空指针导致输出侧输出流失败,
                 *    而out.collect()能成功呢?
                 * A1:
                 * */
                System.out.println("ctx:" + ctx + "\nvalue:" + value);
                ctx.output(noMatch, value);
            }
        }

        @Override
        public void processElement(SensorReading value, ProcessFunction<SensorReading, String>.Context ctx, Collector<String> out) throws Exception {
            /*
             * Explain
             *  测试数据,若id为s1就假装为正常数据输出,若为其他id就假装为未匹配上数据
             * */
            if ("s1".equals(value.getId())) {
                out.collect(value.toString());
            } else {
                //show(ctx,value);
                new Timer().schedule(new myTask(ctx, value), 2000);
            }
        }
    }
}
