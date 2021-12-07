package com.atguigu.table.time.groupwindows;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @ClassName FlinkDemo-table_event_window_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日14:27 - 周二
 * @Describe 滑动窗口
 */
public class table_event_window_5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // step-1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // step-2. 指定事件时间戳为timeStamp字段
        DataStream<SensorReading> dataStream = env.fromElements(new SensorReading("sensor_1", 1000L, 10D),
                new SensorReading("sensor_1", 2000L, 20D),
                new SensorReading("sensor_2", 3000L, 30D),
                new SensorReading("sensor_1", 4000L, 40D),
                new SensorReading("sensor_1", 5000L, 50D),
                new SensorReading("sensor_2", 6000L, 60D)).assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(((element, recordTimestamp) -> element.getTimeStamp() * 1000L)));


        //step-3. 在环境中注册表
        Table table = tableEnv.fromDataStream(dataStream, $("id"), $("timeStamp"), $("temperature"), $("dt").rowtime());

        //step-4. TableAPI查询
        Table resTable = table.window(Tumble.over(
                                lit(10).seconds())
                        .on($("dt"))
                        .as("tw"))
                .groupBy($("id"), $("tw"))
                .select($("id"), $("id").count().as("cnt"), $("temperature").avg().as("tAvg"), $("tw").end());

        table.execute().print();
        env.execute();
    }
}
