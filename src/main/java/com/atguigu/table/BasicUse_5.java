package com.atguigu.table;

import com.atguigu.source.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-BasicUse_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月28日21:46 - 周日
 * @Describe
 */
public class BasicUse_5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        //todo 1.创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());

        //todo 2.创建表：将流转换成动态表，表的字段从pojo属性名自动抽取
        Table inputTable = tableEnv.fromDataStream(dataStream);

        //todo 3.对动态表进行查询
        Table resultTable = inputTable.select($("id"), $("timeStamp"), $("temperature"));

        tableEnv.createTemporaryView("Sensor",dataStream);
        Table sqlTable = tableEnv.sqlQuery("select id,count(1) as cnt from Sensor group by id ");

        //若涉及到数据的更新和改变，要用到撤回流，多了个boolean标记
        //tableEnv.toAppendStream(resultTable, Row.class).print("table API");
        tableEnv.toRetractStream(sqlTable, Row.class).print("Sql");


        env.execute();
    }
}
