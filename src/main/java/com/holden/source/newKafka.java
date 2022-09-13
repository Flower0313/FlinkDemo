package com.holden.source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.holden.bean.SensorReading;
import lombok.Builder;
import lombok.Data;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * @ClassName FlinkDemo-newKafka
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年9月13日18:59 - 周二
 * @Describe
 */
public class newKafka {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("43.142.117.50:9092,43.142.75.17:9092")
                .setTopics("test")
                .setGroupId("flink")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafka_source = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        KeyedStream<Stock, String> keyedStream = kafka_source.map(x -> {
            JSONObject stockJson = JSON.parseObject(x);
            return Stock.builder()
                    .code(stockJson.getString("code"))
                    .name(stockJson.getString("name"))
                    .date(stockJson.getString("date"))
                    .time(stockJson.getString("time"))
                    .buy1(NumberUtils.createBigDecimal(stockJson.getString("buy1")))
                    .buy1hand(Long.valueOf(stockJson.getString("buy1hand")))
                    .sale1(NumberUtils.createBigDecimal(stockJson.getString("sale1")))
                    .sale1hand(Long.valueOf(stockJson.getString("sale1hand")))
                    .build();
        }).keyBy(Stock::getCode);

        OutputTag<String> buyAlertTag = new OutputTag<String>("buyAlert") {
        };
        keyedStream.print();

        keyedStream.process(new KeyedProcessFunction<String, Stock, Object>() {
            private ValueState<BigDecimal> buy1State;
            private ValueState<BigDecimal> sale1State;
            private ValueState<Long> buy1HandState;
            private ValueState<Long> sale1HandState;
            private ValueState<Long> continuityState;

            @Override
            public void open(Configuration parameters) throws Exception {
                buy1State = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("buy1_state", BigDecimal.class));
                sale1State = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sale1_state", BigDecimal.class));
                buy1HandState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("buy1HandState", Long.class));
                sale1HandState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("sale1HandState", Long.class));
                continuityState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("continuityState", Long.class, 0L));
            }

            @Override
            public void close() throws Exception {
                buy1State.clear();
                sale1State.clear();
                buy1HandState.clear();
                sale1HandState.clear();
                continuityState.clear();
            }

            @Override
            public void processElement(Stock value, KeyedProcessFunction<String, Stock, Object>.Context ctx, Collector<Object> out) throws Exception {
                if (buy1State.value() != null || sale1State.value() != null) {
                    if (value.getBuy1().compareTo(buy1State.value()) > 0) {
                        if (continuityState.value() >= 4) {
                            ctx.output(buyAlertTag, value.getCode());
                        }
                        // 连续性+1
                        continuityState.update(continuityState.value() + 1);
                    } else {
                        continuityState.update(0L);
                    }
                    out.collect(value);
                }
            }
        }).print();


        env.execute();
    }
}


@Data
@Builder
class Stock {
    private String name;
    private String code;
    private String date;
    private String time;
    private BigDecimal buy1;
    private Long buy1hand;
    private BigDecimal sale1;
    private Long sale1hand;
}