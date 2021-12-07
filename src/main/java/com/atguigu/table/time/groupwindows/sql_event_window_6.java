package com.atguigu.table.time.groupwindows;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @ClassName FlinkDemo-sql_event_window_6
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日14:35 - 周二
 * @Describe 滑动窗口
 */
public class sql_event_window_6 {
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


        Table sensorTable = tableEnv.fromDataStream(dataStream, $("id"), $("timeStamp"), $("temperature"), $("dt").rowtime());

        //todo 注册临时表
        tableEnv.createTemporaryView("sensor", sensorTable);

        Table table = tableEnv.sqlQuery("select id,count(id) as cnt," +
                "avg(temperature) as avgTmp," +
                "tumble_end(dt,interval '10' second)" +
                "from sensor group by id,tumble(dt,interval '10' second)");

        table.execute().print();

        env.execute();
    }
}
