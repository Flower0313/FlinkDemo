package com.holden.cdc;

import com.alibaba.fastjson.JSONObject;
import com.holden.bean.OdsStock;
import com.holden.bean.StockMid;
import com.mysql.cj.jdbc.Driver;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * @ClassName FlinkDemo-SAR
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月21日18:50 - 周二
 * @Describe
 */
public class SAR {
    public static void main(String[] args) throws Exception {
        //访问本地
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        DebeziumSourceFunction<String> mysql = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("spider_base")
                .tableList("spider_base.ods_stock")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyCustomerDeserialization())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(mysql);

        //转为POJO类并且keyBy根据code分区
        KeyedStream<OdsStock, String> keyedStream = mysqlDS.map(x -> {
            JSONObject jsonObject = JSONObject.parseObject(x);
            JSONObject data = jsonObject.getJSONObject("data");
            return OdsStock.builder().code(data.getString("code"))
                    .name(data.getString("name"))
                    .closing_price(data.getBigDecimal("closing_price"))
                    .last_closing(data.getBigDecimal("last_closing"))
                    .date(data.getString("date"))
                    .deal_amount(data.getBigDecimal("deal_amount"))
                    .rk(data.getInteger("rk"))
                    .x(data.getBigDecimal("x"))
                    .i(data.getBigDecimal("i"))
                    .rsv(data.getBigDecimal("rsv"))
                    .highest(data.getBigDecimal("highest"))
                    .lowest(data.getBigDecimal("lowest"))
                    .table(jsonObject.getString("table"))
                    .sar_high(data.getBigDecimal("sar_high"))
                    .sar_low(data.getBigDecimal("sar_low"))
                    .build();
        }).keyBy(OdsStock::getCode);

        SingleOutputStreamOperator<StockMid> result = keyedStream.map(new RichMapFunction<OdsStock, StockMid>() {
            private ValueState<BigDecimal> sar_af;
            private ValueState<BigDecimal> sar;
            private ValueState<BigDecimal> sar_high;
            private ValueState<BigDecimal> sar_low;
            private ValueState<Boolean> sar_bull;

            @Override
            public void open(Configuration parameters) throws Exception {
                sar_af = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sar_state", BigDecimal.class));
                sar = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sar_af_state", BigDecimal.class));
                sar_high = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sar_high_state", BigDecimal.class));
                sar_low = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sar_low_state", BigDecimal.class));
                sar_bull = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("sar_bull_state", Boolean.class));

            }

            @Override
            public void close() throws Exception {
                sar.clear();
                sar_af.clear();
                sar_high.clear();
                sar_low.clear();
            }

            @Override
            public StockMid map(OdsStock value) throws Exception {
                if (value.getRk() == 4 || value.getRk() == 5) {
                    sar.update(value.getSar_low());
                    sar_high.update(value.getSar_high());
                    sar_low.update(value.getSar_low());
                    sar_af.update(new BigDecimal("0.02"));
                    sar_bull.update(true);
                    System.out.println(value.getRk()+"|sar:" + value.getSar_low() + "|sar_bull:" + sar_bull.value() + "|sar_high:" + sar_high.value() + "|sar_low:" + sar_low.value() + "|sar_af:" + sar_af.value());
                } else if (value.getRk() > 5) {
                    if (sar_bull.value()) {
                        BigDecimal tmp_sar = sar.value().add(sar_af.value().multiply(sar_high.value().subtract(sar.value())));
                        if (value.getHighest().compareTo(sar_high.value()) > 0) {
                            //更新sar_high
                            sar_high.update(value.getHighest());
                            //更新sar_af
                            if ((sar_af.value().add(new BigDecimal("0.02"))).compareTo(new BigDecimal("0.2")) > 0) {
                                sar_af.update(new BigDecimal("0.2"));
                            } else {
                                sar_af.update(sar_af.value().add(new BigDecimal("0.02")));
                            }
                        }
                        sar.update(tmp_sar);
                        if (tmp_sar.compareTo(value.getClosing_price()) > 0) {
                            sar.update(value.getSar_high());
                            sar_af.update(new BigDecimal("0.02"));
                            sar_bull.update(false);
                            sar_high.update(value.getSar_high());
                            sar_low.update(value.getSar_low());
                            System.out.println(value.getRk()+"|sar:" + value.getSar_high() + "|sar_bull:" + sar_bull.value() + "|sar_high:" + sar_high.value() + "|sar_low:" + sar_low.value() + "|sar_af:" + sar_af.value());
                        }else{
                            System.out.println(value.getRk()+"|sar:" + tmp_sar + "|sar_bull:" + sar_bull.value() + "|sar_high:" + sar_high.value() + "|sar_low:" + sar_low.value() + "|sar_af:" + sar_af.value());
                        }

                    } else {
                        BigDecimal tmp_sar = sar.value().add(sar_af.value().multiply(sar_low.value().subtract(sar.value())));
                        if (value.getLowest().compareTo(sar_low.value()) < 0) {
                            //更新sar_high
                            sar_low.update(value.getLowest());
                            //更新sar_af
                            if ((sar_af.value().add(new BigDecimal("0.02"))).compareTo(new BigDecimal("0.2")) > 0) {
                                sar_af.update(new BigDecimal("0.2"));
                            } else {
                                sar_af.update(sar_af.value().add(new BigDecimal("0.02")));
                            }
                        }
                        sar.update(tmp_sar);
                        if (tmp_sar.compareTo(value.getClosing_price()) < 0) {
                            sar.update(value.getSar_low());
                            sar_af.update(new BigDecimal("0.02"));
                            sar_bull.update(true);
                            sar_high.update(value.getSar_high());
                            sar_low.update(value.getSar_low());
                            System.out.println(value.getRk()+"|sar:" + value.getSar_low() + "|sar_bull:" + sar_bull.value() + "|sar_high:" + sar_high.value() + "|sar_low:" + sar_low.value() + "|sar_af:" + sar_af.value());
                        }else{
                            System.out.println(value.getRk()+"|sar:" + tmp_sar+ "|sar_bull:" + sar_bull.value() + "|sar_high:" + sar_high.value() + "|sar_low:" + sar_low.value() + "|sar_af:" + sar_af.value());
                        }
                    }
                }
                return new StockMid();
            }
        });


        env.execute();
    }
}
