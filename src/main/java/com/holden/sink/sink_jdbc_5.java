package com.holden.sink;

import com.mysql.jdbc.Driver;
import com.holden.bean.SensorReading;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

import static com.holden.common.CommonEnv.JDBC;
import static com.holden.common.CommonEnv.SQL_PASSWORD;

/**
 * @ClassName FlinkDemo-sink_jdbc_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月05日13:39 - 周日
 * @Describe
 */
public class sink_jdbc_5 {
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

        dataStream.addSink(JdbcSink.sink(
                "insert into sensor values (?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.getId());
                    ps.setLong(2, t.getTimeStamp());
                    ps.setBigDecimal(3,
                            new BigDecimal(t.getTemperature().toString()));
                },
                new JdbcExecutionOptions.Builder()
                        //设置来一条写一条与ES中相似
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(JDBC)
                        .withUsername("root")
                        .withPassword(SQL_PASSWORD)
                        .withDriverName(Driver.class.getName())
                        .build()
        ));


        env.execute();
    }
}
