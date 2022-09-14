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
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

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
                    .opening_price(NumberUtils.createBigDecimal(stockJson.getString("opening_price")))
                    .current_price(NumberUtils.createBigDecimal(stockJson.getString("current_price")))
                    .before_closing(NumberUtils.createBigDecimal(stockJson.getString("before_closing")))
                    .highest(NumberUtils.createBigDecimal(stockJson.getString("highest")))
                    .lowest(NumberUtils.createBigDecimal(stockJson.getString("lowest")))
                    .date(stockJson.getString("date"))
                    .time(stockJson.getString("time"))
                    .buy1(NumberUtils.createBigDecimal(stockJson.getString("buy1")))
                    .buy2(NumberUtils.createBigDecimal(stockJson.getString("buy2")))
                    .buy3(NumberUtils.createBigDecimal(stockJson.getString("buy3")))
                    .buy4(NumberUtils.createBigDecimal(stockJson.getString("buy4")))
                    .buy5(NumberUtils.createBigDecimal(stockJson.getString("buy5")))
                    .buy1hand(Long.valueOf(stockJson.getString("buy1hand")))
                    .buy2hand(Long.valueOf(stockJson.getString("buy2hand")))
                    .buy3hand(Long.valueOf(stockJson.getString("buy3hand")))
                    .buy4hand(Long.valueOf(stockJson.getString("buy4hand")))
                    .buy5hand(Long.valueOf(stockJson.getString("buy5hand")))
                    .sale1(NumberUtils.createBigDecimal(stockJson.getString("sale1")))
                    .sale1(NumberUtils.createBigDecimal(stockJson.getString("sale2")))
                    .sale1(NumberUtils.createBigDecimal(stockJson.getString("sale3")))
                    .sale1(NumberUtils.createBigDecimal(stockJson.getString("sale4")))
                    .sale1(NumberUtils.createBigDecimal(stockJson.getString("sale5")))
                    .sale1hand(Long.valueOf(stockJson.getString("sale1hand")))
                    .sale1hand(Long.valueOf(stockJson.getString("sale2hand")))
                    .sale1hand(Long.valueOf(stockJson.getString("sale3hand")))
                    .sale1hand(Long.valueOf(stockJson.getString("sale4hand")))
                    .sale1hand(Long.valueOf(stockJson.getString("sale5hand")))
                    .build();
        }).keyBy(Stock::getCode);

        OutputTag<String> buyAlertTag = new OutputTag<String>("buyAlert") {
        };

        SingleOutputStreamOperator<Object> process = keyedStream.process(new KeyedProcessFunction<String, Stock, Object>() {
            private ValueState<BigDecimal> buy1State;
            private ValueState<BigDecimal> sale1State;
            private ValueState<Long> buy1HandState;
            private ValueState<Long> sale1HandState;
            private ValueState<Long> continuityState;
            private ValueState<Long> timeState;
            private ValueState<String> codeState;
            private ValueState<String> status;

            @Override
            public void open(Configuration parameters) throws Exception {
                buy1State = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("buy1_state", BigDecimal.class, BigDecimal.ZERO));
                sale1State = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sale1_state", BigDecimal.class, BigDecimal.ZERO));
                buy1HandState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("buy1HandState", Long.class));
                sale1HandState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("sale1HandState", Long.class));
                continuityState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("continuityState", Long.class, 0L));
                timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState", Long.class));
                codeState = getRuntimeContext().getState(new ValueStateDescriptor<String>("codeState", String.class));
                status = getRuntimeContext().getState(new ValueStateDescriptor<String>("status", String.class));
            }

            @Override
            public void close() throws Exception {
                buy1State.clear();
                sale1State.clear();
                buy1HandState.clear();
                sale1HandState.clear();
                continuityState.clear();
                timeState.clear();
                codeState.clear();
                status.clear();
            }

            @Override
            public void processElement(Stock value, KeyedProcessFunction<String, Stock, Object>.Context ctx, Collector<Object> out) throws Exception {
                codeState.update(value.getCode());
                status.update(value.getCurrent_price().subtract(value.getBefore_closing()).multiply(BigDecimal.valueOf(100L)).divide(value.getBefore_closing(), 4, RoundingMode.HALF_UP).toString() + "%");
                if (value.getBuy1().compareTo(buy1State.value()) > 0) {
                    // 注册10秒定时器
                    long timestamp = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerProcessingTimeTimer(timestamp + 9999L);
                    timeState.update(timestamp + 9999L);
                    // 连续性+1
                    continuityState.update(continuityState.value() + 1);

                } else {
                    continuityState.update(0L);
                    //取消定时器
                    if (timeState.value() != null) {
                        ctx.timerService().deleteProcessingTimeTimer(timeState.value());
                    }
                    timeState.clear();
                }
                buy1State.update(value.getBuy1());
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Stock, Object>.OnTimerContext ctx, Collector<Object> out) throws Exception {
                if (continuityState.value() >= 3) {
                    ctx.output(buyAlertTag, codeState.value() + "--" + status.value());
                }
            }
        });
        process.getSideOutput(buyAlertTag).print("实时高频");


        env.execute();
    }
}


@Data
@Builder
class Stock {
    private String name;
    private String code;
    private BigDecimal opening_price;
    private BigDecimal current_price;
    private BigDecimal before_closing;
    private BigDecimal highest;
    private BigDecimal lowest;
    private String date;
    private String time;
    private BigDecimal buy1;
    private BigDecimal buy2;
    private BigDecimal buy3;
    private BigDecimal buy4;
    private BigDecimal buy5;
    private Long buy1hand;
    private Long buy2hand;
    private Long buy3hand;
    private Long buy4hand;
    private Long buy5hand;
    private BigDecimal sale1;
    private BigDecimal sale2;
    private BigDecimal sale3;
    private BigDecimal sale4;
    private BigDecimal sale5;
    private Long sale1hand;
    private Long sale2hand;
    private Long sale3hand;
    private Long sale4hand;
    private Long sale5hand;
}