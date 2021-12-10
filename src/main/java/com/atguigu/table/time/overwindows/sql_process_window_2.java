package com.atguigu.table.time.overwindows;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-sql_process_window_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日16:06 - 周二
 * @Describe
 */
public class sql_process_window_2 {
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

        // step-4. 本质就是mysql中的窗口函数
        Table table = tableEnv.sqlQuery(
                "select id," +
                        "count(id) over (partition by id order by rt rows between 2 preceding and current row)" +
                        "from sensor");

        table.execute().print();
        env.execute();
    }
}
