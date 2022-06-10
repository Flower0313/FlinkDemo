package com.holden.table.time.groupwindows;

import com.holden.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-sql_process_window_4
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日14:08 - 周二
 * @Describe 滑动窗口
 */
public class sql_process_window_4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // step-1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // step-2. 将DataStream -> Table
        Table sensorTable = tableEnv.fromDataStream(dataStream, $("id"), $("timeStamp"), $("temperature"), $("rt").proctime());

        // step-3. 在环境中注册表
        tableEnv.createTemporaryView("sensor", sensorTable);

        Table table = tableEnv.sqlQuery("select id,count(id) as cnt," +
                "avg(temperature) as avgTmp," +
                "tumble_end(rt,interval '10' second)" +
                "from sensor group by id,tumble(rt,interval '10' second)");


        table.execute().print();

        tableEnv.toAppendStream(table,Row.class).print();
        env.execute();
    }
}
