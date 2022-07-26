package com.holden.cdc;

import com.alibaba.fastjson.JSONObject;
import com.holden.bean.Mike;
import com.holden.bean.PreMike;
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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static com.holden.cdc.ConnConfig.*;

/**
 * @ClassName FlinkDemo-Mike
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年7月26日19:16 - 周二
 * @Describe
 */
public class MikeClass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DebeziumSourceFunction<String> mysql = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("spider_base")
                .tableList("spider_base.ods_mike")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyCustomerDeserialization())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(mysql);

        KeyedStream<PreMike, String> keyedStream = mysqlDS.map(x -> {
            JSONObject jsonObject = JSONObject.parseObject(x);
            JSONObject data = jsonObject.getJSONObject("data");
            return PreMike.builder()
                    .code(data.getString("code"))
                    .name(data.getString("name"))
                    .ds(data.getString("ds"))
                    .rk(data.getInteger("rk"))
                    .hhv(data.getBigDecimal("hhv"))
                    .llv(data.getBigDecimal("llv"))
                    .highest(data.getBigDecimal("highest"))
                    .lowest(data.getBigDecimal("lowest"))
                    .hlc(data.getBigDecimal("hlc"))
                    .build();
        }).keyBy(PreMike::getCode);

        keyedStream.map(new RichMapFunction<PreMike, Mike>() {
            private MapState<String, MikeClass> MikeState;
            private ValueState<BigDecimal> hv_state;
            private ValueState<BigDecimal> lv_state;
            private ValueState<BigDecimal> stor_state;
            private ValueState<BigDecimal> midr_state;
            private ValueState<BigDecimal> wekr_state;
            private ValueState<BigDecimal> weks_state;
            private ValueState<BigDecimal> mids_state;
            private ValueState<BigDecimal> stos_state;


            @Override
            public void open(Configuration parameters) throws Exception {
                hv_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("hv_state", BigDecimal.class));
                lv_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("lv_high_state", BigDecimal.class));
                stor_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("stor_state", BigDecimal.class));
                midr_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("midr_state", BigDecimal.class));
                wekr_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("wekr_state", BigDecimal.class));
                weks_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("weks_state", BigDecimal.class));
                mids_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("mids_state", BigDecimal.class));
                stos_state = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("stos_state", BigDecimal.class));
            }

            @Override
            public void close() throws Exception {
                hv_state.clear();
                lv_state.clear();
                stor_state.clear();
                midr_state.clear();
                wekr_state.clear();
                weks_state.clear();
                mids_state.clear();
                stos_state.clear();
            }

            @Override
            public Mike map(PreMike value) throws Exception {
                Mike mike = new Mike();
                BigDecimal hv = BigDecimal.ZERO;
                BigDecimal lv = BigDecimal.ZERO;
                BigDecimal stor = BigDecimal.ZERO;
                BigDecimal stos = BigDecimal.ZERO;
                BigDecimal midr = BigDecimal.ZERO;
                BigDecimal wekr = BigDecimal.ZERO;
                BeanUtils.copyProperties(mike, value);

                if (value.getRk() == 1) {
                    hv = value.getHhv();
                    lv = value.getLlv();
                    stor = hv.multiply(BigDecimal.valueOf(2)).subtract(lv);
                    stos = lv.multiply(BigDecimal.valueOf(2)).subtract(hv);
                } else {
                    hv = hv_state.value().multiply(BigDecimal.valueOf(2)).add(value.getHhv().multiply(EMA3ZI)).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                    lv = lv_state.value().multiply(BigDecimal.valueOf(2)).add(value.getLlv().multiply(EMA3ZI)).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                    stor = hv.multiply(BigDecimal.valueOf(2)).subtract(lv).multiply(BigDecimal.valueOf(2)).add(EMA3ZI.multiply(stor_state.value())).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                    stos = lv.multiply(BigDecimal.valueOf(2)).subtract(hv).multiply(BigDecimal.valueOf(2)).add(EMA3ZI.multiply(stos_state.value())).divide(EMA3MU, 6, RoundingMode.HALF_UP);
                    if (value.getRk() == 11) {
                        midr = value.getHlc().add(hv).subtract(lv);
                        wekr = value.getHlc().multiply(BigDecimal.valueOf(2)).subtract(lv);


                    }
                }

                hv_state.update(hv);
                lv_state.update(lv);
                stor_state.update(stor);
                stos_state.update(stos);
                midr_state.update(midr);
                wekr_state.update(wekr);
                mike.setSTOR(stor);
                mike.setSTOS(stos);
                mike.setMIDR(midr);
                mike.setWEKR(wekr);
                return mike;
            }
        }).print();


        env.execute();
    }
}
