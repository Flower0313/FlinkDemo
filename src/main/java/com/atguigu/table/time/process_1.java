package com.atguigu.table.time;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-process_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日1:10 - 周二
 * @Describe 定义本地时间戳
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/table/streaming/time_attributes.html
 */
public class process_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SensorReading> waterSensorStream =
                env.fromElements(new SensorReading("sensor_1", 1000L, 10D),
                        new SensorReading("sensor_1", 2000L, 20D),
                        new SensorReading("sensor_2", 3000L, 30D),
                        new SensorReading("sensor_1", 4000L, 40D),
                        new SensorReading("sensor_1", 5000L, 50D),
                        new SensorReading("sensor_2", 6000L, 60D));

        // step-1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // step-2. 将流转换为表，定义一个时间特性
        //todo 方式一
        Table sensorTable = tableEnv.fromDataStream(waterSensorStream, $("id"), $("timeStamp"), $("temperature"), $("pt").proctime());


        //todo 方式二 (不建议,有小bug)
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("timeStamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.DOUBLE())
                .field("pt", DataTypes.TIMESTAMP()).proctime();

        //todo 方式三(推荐写法)
        String sourceDDL =
                "create table sensor_source(id STRING," +
                        "`timeStamp` BIGINT," +
                        "temperature DOUBLE" +
                        "pt as PROCTIME()" +
                        ") with (" +
                        "'connector'='filesystem'," +
                        "'format'='csv'," +
                        "'path'='T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt'" +
                        ")";


        //step-3 输出
        tableEnv.toAppendStream(sensorTable, Row.class).print();

        //查看字段类型
        sensorTable.printSchema();

        //sensorTable.execute().print();
        env.execute();

    }
}
