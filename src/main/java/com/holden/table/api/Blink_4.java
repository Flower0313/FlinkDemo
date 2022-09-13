package com.holden.table.api;

import com.holden.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName FlinkDemo-old_planner_4
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月28日20:15 - 周日
 * @Describe
 */
public class Blink_4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


       /* //基于Blink的流处理
        EnvironmentSettings blinkStreamingSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment streamEnv = StreamTableEnvironment.create(env, blinkStreamingSetting);

        //基于Blink的批处理
        EnvironmentSettings blinkBatchSetting = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        //attention 不需要传env了,因为env是属于流处理的
        TableEnvironment batchEnv = TableEnvironment.create(blinkBatchSetting);*/

        env.execute();
    }
}
