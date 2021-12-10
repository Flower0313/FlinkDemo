package com.atguigu.table.sql;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName FlinkDemo-sql_base_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日22:28 - 周一
 * @Describe
 */
public class sql_base_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //step-1 建造数据源
        DataStreamSource<SensorReading> waterSensorStream =
                env.fromElements(new SensorReading("sensor_1", 1000L, 10D),
                        new SensorReading("sensor_1", 2000L, 20D),
                        new SensorReading("sensor_2", 3000L, 30D),
                        new SensorReading("sensor_1", 4000L, 40D),
                        new SensorReading("sensor_1", 5000L, 50D),
                        new SensorReading("sensor_2", 6000L, 60D));

        //step-2 使用sql查询未注册的表
        Table inputTable = tEnv.fromDataStream(waterSensorStream);
        Table resTable = tEnv.sqlQuery("select * from " + inputTable + " where id='sensor_1'");
        tEnv.toAppendStream(resTable, Row.class).print();

        //step-3 使用sql查询一个已注册的表
        //todo 方式一
        Table inputTable2 = tEnv.fromDataStream(waterSensorStream);
        //explain 给tEnv环境注册表,是否注册了表就看这一步了
        tEnv.createTemporaryView("sensor", inputTable2);
        Table resTable2 = tEnv.sqlQuery("select * from sensor where id='sensor_1'");
        tEnv.toAppendStream(resTable2, Row.class).print();

        //todo 方式二
        tEnv.createTemporaryView("inputTable",waterSensorStream);
        Table sensorTable = tEnv.from("inputTable");

        env.execute();
    }
}
