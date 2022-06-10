package com.holden.table.time.overwindows;

import com.holden.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @ClassName FlinkDemo-sql_process_window_4
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日14:08 - 周二
 * @Describe 官方文档:https://nightlies.apache.org/flink/flink-docs-release-1.13/zh/docs/dev/table/tableapi/
 */
public class table_process_window_1 {
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
        Table table = sensorTable.window(Over.partitionBy($("id"))
                        .orderBy($("rt"))
                        .preceding(rowInterval(2L))//等价于preceding("2.rows")聚合窗口中前两行的数据值
                        .as("ow"))
                .select($("id"),
                        $("id").count().over($("ow")));

        table.execute().print();
        //tableEnv.toAppendStream(table, Row.class).print();
        env.execute();
    }
}
