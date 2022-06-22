package com.holden.bean;

import com.alibaba.fastjson.JSONObject;
import com.holden.cdc.MyCustomerDeserialization;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;

/**
 * @ClassName FlinkDemo-bing
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月22日18:37 - 周三
 * @Describe
 */
public class bing {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> inputStream313 = env.socketTextStream("hadoop102", 31313);

        DataStream<SensorReading> dataStream13 = inputStream313.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        KeyedStream<SensorReading, String> keyedStream = dataStream13.keyBy(SensorReading::getId);

        keyedStream.map(new RichMapFunction<SensorReading, String>() {
            private ValueState<String> testState;

            @Override
            public void open(Configuration parameters) throws Exception {
                testState = getRuntimeContext().getState(new ValueStateDescriptor<String>("test", String.class));

            }

            @Override
            public void close() throws Exception {
                testState.clear();
            }

            @Override
            public String map(SensorReading value) throws Exception {
                System.out.println(testState.value());
                testState.update(value.getId());
                return null;
            }
        });


        env.execute();
    }
}
