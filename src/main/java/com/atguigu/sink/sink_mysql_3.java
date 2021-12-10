package com.atguigu.sink;

import com.atguigu.bean.SensorReading;

import java.math.BigDecimal;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ClassName FlinkDemo-sink_mysql_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月05日12:35 - 周日
 * @Describe
 */
public class sink_mysql_3 {
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

        dataStream.addSink(new RichSinkFunction<SensorReading>() {
            private PreparedStatement ps;
            private Connection conn;

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("连接数据库");
                conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root", "root");
                ps = conn.prepareStatement("insert into sensor values(?, ?, ?)");
            }

            @Override
            public void close() throws Exception {
                ps.close();
                conn.close();
            }

            @Override
            public void invoke(SensorReading value, Context context) throws Exception {
                System.out.println("sql执行语句");
                ps.setString(1, value.getId());
                ps.setLong(2, value.getTimeStamp());
                ps.setBigDecimal(3,
                        new BigDecimal(value.getTemperature().toString()));
                ps.executeUpdate();

            }
        });


        env.execute();
    }
}
