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
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

import static com.holden.cdc.ConnConfig.*;


/**
 * @ClassName FlinkDemo-ddd
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月13日16:59 - 周一
 * @Describe
 */
public class ddd {
    public static void main(String[] args) throws Exception {
        //访问本地
        //若调成多并行度，那么rk=10可能会先进来，虽然状态是共享的，但是我们的取9的操作就为null了，所以并行度必须为1，因为我这是特殊情况，必须要有序
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        //将状态保存在文件中
        //env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///T:\\ShangGuiGu\\FlinkDemo\\output\\statebackend"));
        DebeziumSourceFunction<String> mysql = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("spider_base")
                .tableList("spider_base.ods_stock_step_one")
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
                    .tr(data.getBigDecimal("tr"))
                    .dmm(data.getBigDecimal("dmm"))
                    .dmp(data.getBigDecimal("dmp"))
                    .build();
        }).keyBy(OdsStock::getCode);


        SingleOutputStreamOperator<StockMid> result = keyedStream.map(new RichMapFunction<OdsStock, StockMid>() {
            private MapState<String, StockMid> stockState;
            private ValueState<BigDecimal> sar_af;
            private ValueState<BigDecimal> sar_high;
            private ValueState<BigDecimal> sar_low;
            private ValueState<Boolean> sar_bull;
            private ValueState<BigDecimal> tr_sum;
            private ValueState<BigDecimal> dmp_sum;
            private ValueState<BigDecimal> dmm_sum;
            private ValueState<BigDecimal> tr_ex;
            private ValueState<BigDecimal> dmp_ex;
            private ValueState<BigDecimal> dmm_ex;
            private ValueState<BigDecimal> adx;


            @Override
            public void open(Configuration parameters) throws Exception {
                stockState = getRuntimeContext().getMapState(new MapStateDescriptor<String, StockMid>("my-map", String.class, StockMid.class));
                sar_af = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sar_state", BigDecimal.class));
                sar_high = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sar_high_state", BigDecimal.class));
                sar_low = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("sar_low_state", BigDecimal.class));
                sar_bull = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("sar_bull_state", Boolean.class));
                tr_sum = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("tr_sum_state", BigDecimal.class));
                dmp_sum = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("dmp_sum_state", BigDecimal.class));
                dmm_sum = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("dmm_sum_state", BigDecimal.class));
                tr_ex = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("tr_ex_state", BigDecimal.class));
                dmp_ex = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("dmp_ex_state", BigDecimal.class));
                dmm_ex = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("dmm_ex_state", BigDecimal.class));
                adx = getRuntimeContext().getState(new ValueStateDescriptor<BigDecimal>("adx_state", BigDecimal.class));
            }

            @Override
            public void close() throws Exception {
                stockState.clear();
                sar_af.clear();
                sar_high.clear();
                sar_low.clear();
                sar_bull.clear();
                tr_sum.clear();
                dmp_sum.clear();
                dmm_sum.clear();
                tr_ex.clear();
                dmp_ex.clear();
                dmm_ex.clear();
                adx.clear();
            }

            @Override
            public StockMid map(OdsStock value) throws Exception {
                StockMid stockMid = new StockMid();
                String key = value.getCode() + delimiter + value.getRk();
                //第一条数据
                if (value.getRk() == FIRST_DAY) {
                    BigDecimal highest = value.getHighest();
                    BigDecimal lowest = value.getLowest();
                    BeanUtils.copyProperties(stockMid, value);
                    stockMid.setObv(INITIAL);
                    stockMid.setLast_closing(INITIAL);
                    stockMid.setDif(INITIAL);
                    stockMid.setEma12(value.getClosing_price());
                    stockMid.setEma26(value.getClosing_price());
                    stockMid.setClosing_diff(INITIAL);

                    BigDecimal rsv = BigDecimal.valueOf(0.0);
                    if (!Objects.equals(highest, lowest)) {
                        rsv = (value.getClosing_price().subtract(lowest).multiply(BigDecimal.valueOf(100))).divide(highest.subtract(lowest).equals(BigDecimal.ZERO) ? BigDecimal.ONE : highest.subtract(lowest), SCALE, RoundingMode.HALF_UP);
                        stockMid.setRsv(rsv);
                    } else {
                        stockMid.setRsv(INITIAL);
                    }
                    stockMid.setK(rsv);
                    stockMid.setD(rsv);
                    stockMid.setJ(rsv);
                    stockMid.setUp6(INITIAL);
                    stockMid.setUp12(INITIAL);
                    stockMid.setUp24(INITIAL);
                    stockMid.setDown6(INITIAL);
                    stockMid.setDown12(INITIAL);
                    stockMid.setDown24(INITIAL);
                    stockMid.setDea(INITIAL);
                    stockMid.setMacd(INITIAL);
                    stockMid.setRsi6(INITIAL);
                    stockMid.setRsi12(INITIAL);
                    stockMid.setRsi24(INITIAL);
                    stockState.put(key, stockMid);
                } else {

                    //除开第一条后的数据
                    if (!stockState.isEmpty()) {
                        String last_key = value.getCode() + "-" + (value.getRk() - 1);
                        //手动移除掉前面的状态，不然后面的读取速度会变慢
                        if (value.getRk() >= REMOVE_FLAG) {
                            stockState.remove(value.getCode() + "-" + (value.getRk() - 2));
                        }
                        //取上一条记录
                        StockMid last_Stock = stockState.get(last_key);
                        BeanUtils.copyProperties(stockMid, value);
                        BigDecimal closing_diff = value.getClosing_price().subtract(last_Stock.getClosing_price());
                        BigDecimal ema12 = (BigDecimal.valueOf(2).multiply(value.getClosing_price()).add(BigDecimal.valueOf(11).multiply(last_Stock.getEma12()))).divide(BigDecimal.valueOf(13), SCALE, RoundingMode.HALF_UP);
                        BigDecimal ema26 = (BigDecimal.valueOf(2).multiply(value.getClosing_price()).add(BigDecimal.valueOf(25).multiply(last_Stock.getEma26()))).divide(BigDecimal.valueOf(27), SCALE, RoundingMode.HALF_UP);
                        BigDecimal dif = ema12.subtract(ema26);

                        stockMid.setRsv(value.getRsv());
                        stockMid.setEma12(ema12);
                        stockMid.setEma26(ema26);
                        stockMid.setDif(dif);
                        stockMid.setClosing_diff(closing_diff);
                        stockMid.setLast_closing(last_Stock.getClosing_price());
                        BigDecimal up6;
                        BigDecimal up12;
                        BigDecimal up24;
                        BigDecimal down6;
                        BigDecimal down12;
                        BigDecimal down24;

                        if (closing_diff.compareTo(BigDecimal.valueOf(0)) > 0) {
                            stockMid.setObv(last_Stock.getObv().add(value.getDeal_amount()));
                            up6 = (closing_diff.add(last_Stock.getUp6().multiply(BigDecimal.valueOf(5)))).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            up12 = (closing_diff.add(last_Stock.getUp12().multiply(BigDecimal.valueOf(11)))).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            up24 = (closing_diff.add(last_Stock.getUp24().multiply(BigDecimal.valueOf(23)))).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                            down6 = last_Stock.getDown6().abs().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            down12 = last_Stock.getDown12().abs().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            down24 = last_Stock.getDown24().abs().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                            stockMid.setUp6(up6);
                            stockMid.setUp12(up12);
                            stockMid.setUp24(up24);
                            stockMid.setDown6(down6);
                            stockMid.setDown12(down12);
                            stockMid.setDown24(down24);
                        } else if (closing_diff.compareTo(BigDecimal.valueOf(0)) < 0) {
                            stockMid.setObv(last_Stock.getObv().subtract(value.getDeal_amount()));
                            up6 = last_Stock.getUp6().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            down6 = (closing_diff.abs().add(last_Stock.getDown6().multiply(BigDecimal.valueOf(5)))).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            down12 = (closing_diff.abs().add(last_Stock.getDown12().multiply(BigDecimal.valueOf(11)))).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            up12 = last_Stock.getUp12().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            down24 = (closing_diff.abs().add(last_Stock.getDown24().multiply(BigDecimal.valueOf(23)))).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                            up24 = last_Stock.getUp24().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                            stockMid.setDown6(down6);
                            stockMid.setDown12(down12);
                            stockMid.setDown24(down24);
                            stockMid.setUp6(up6);
                            stockMid.setUp12(up12);
                            stockMid.setUp24(up24);
                        } else {
                            stockMid.setObv(last_Stock.getObv());
                            down6 = last_Stock.getDown6().abs().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            up6 = last_Stock.getUp6().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            down12 = last_Stock.getDown12().abs().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            up12 = last_Stock.getUp12().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), SCALE, RoundingMode.HALF_UP);
                            down24 = last_Stock.getDown24().abs().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                            up24 = last_Stock.getUp24().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), SCALE, RoundingMode.HALF_UP);
                            stockMid.setDown6(down6);
                            stockMid.setDown12(down12);
                            stockMid.setDown24(down24);
                            stockMid.setUp6(up6);
                            stockMid.setUp12(up12);
                            stockMid.setUp24(up24);
                        }
                        BigDecimal k = (value.getRsv().add(last_Stock.getK().multiply(BigDecimal.valueOf(2)))).divide(BigDecimal.valueOf(3), SCALE, RoundingMode.HALF_UP);
                        BigDecimal d = (k.add(last_Stock.getD().multiply(BigDecimal.valueOf(2)))).divide(BigDecimal.valueOf(3), SCALE, RoundingMode.HALF_UP);
                        stockMid.setK(k);
                        stockMid.setD(d);
                        stockMid.setJ(k.multiply(BigDecimal.valueOf(3)).subtract(d.multiply(BigDecimal.valueOf(2))));
                        BigDecimal dea = dif.multiply(BigDecimal.valueOf(2)).add(last_Stock.getDea().multiply(BigDecimal.valueOf(8))).divide(BigDecimal.valueOf(10), SCALE, RoundingMode.HALF_UP);
                        stockMid.setDea(dea);
                        stockMid.setMacd(dif.subtract(dea).multiply(BigDecimal.valueOf(2)));
                        /*
                         * Explain
                         *   这里不能使用.equals(Bigdecimal.ZERO),因为这里只比较0，而不能比较0.0
                         * */
                        stockMid.setRsi6(up6.divide(up6.add(down6).doubleValue() == 0 ? new BigDecimal(1) : up6.add(down6), SCALE, RoundingMode.HALF_UP));
                        stockMid.setRsi12(up12.divide(up12.add(down12).doubleValue() == 0 ? new BigDecimal(1) : up12.add(down12), SCALE, RoundingMode.HALF_UP));
                        stockMid.setRsi24(up24.divide(up24.add(down24).doubleValue() == 0 ? new BigDecimal(1) : up24.add(down24), SCALE, RoundingMode.HALF_UP));

                        //Holden SAR指标
                        if (value.getRk() == SAR_START_FLAG || value.getRk() == (SAR_START_FLAG + 1)) {
                            stockMid.setSar(value.getSar_low());
                            sar_high.update(value.getSar_high());
                            sar_low.update(value.getSar_low());
                            sar_af.update(SAR_AF);
                            sar_bull.update(true);
                        } else if (value.getRk() > (SAR_START_FLAG + 1)) {
                            if (sar_bull.value()) {
                                BigDecimal tmp_sar = last_Stock.getSar().add(sar_af.value().multiply(sar_high.value().subtract(last_Stock.getSar())));
                                if (value.getHighest().compareTo(sar_high.value()) > 0) {
                                    //更新sar_high
                                    sar_high.update(value.getHighest());
                                    //更新sar_af
                                    sar_af.update(BigDecimal.valueOf(0.2).min((sar_af.value().add(SAR_AF))));
                                }
                                stockMid.setSar(tmp_sar);
                                if (tmp_sar.compareTo(value.getClosing_price()) > 0) {
                                    stockMid.setSar(value.getSar_high());
                                    sar_af.update(SAR_AF);
                                    sar_bull.update(false);
                                    sar_high.update(value.getSar_high());
                                    sar_low.update(value.getSar_low());
                                }
                            } else {
                                BigDecimal tmp_sar = last_Stock.getSar().add(sar_af.value().multiply(sar_low.value().subtract(last_Stock.getSar())));
                                if (value.getLowest().compareTo(sar_low.value()) < 0) {
                                    //更新sar_high
                                    sar_low.update(value.getLowest());
                                    //更新sar_af
                                    sar_af.update(BigDecimal.valueOf(0.2).min((sar_af.value().add(SAR_AF))));
                                }
                                stockMid.setSar(tmp_sar);
                                if (tmp_sar.compareTo(value.getClosing_price()) < 0) {
                                    stockMid.setSar(value.getSar_low());
                                    sar_af.update(SAR_AF);
                                    sar_bull.update(true);
                                    sar_high.update(value.getSar_high());
                                    sar_low.update(value.getSar_low());
                                }
                            }
                        }
                        stockState.put(key, stockMid);
                    }
                }
                //DMI
                BigDecimal trex = null;
                BigDecimal dmpex = null;
                BigDecimal dmmex = null;
                BigDecimal mpdi = null;
                if (value.getRk() < DMI_START_FLAG) {//13天含以前
                    tr_sum.update((tr_sum.value() == null ? BigDecimal.valueOf(0) : tr_sum.value()).add(value.getTr()));
                    dmp_sum.update((dmp_sum.value() == null ? BigDecimal.valueOf(0) : dmp_sum.value()).add(value.getDmp()));
                    dmm_sum.update((dmm_sum.value() == null ? BigDecimal.valueOf(0) : dmm_sum.value()).add(value.getDmm()));
                } else {//14天含以后
                    if (value.getRk() == DMI_START_FLAG) {
                        tr_sum.update((tr_sum.value() == null ? BigDecimal.valueOf(0) : tr_sum.value()).add(value.getTr()));
                        dmp_sum.update((dmp_sum.value() == null ? BigDecimal.valueOf(0) : dmp_sum.value()).add(value.getDmp()));
                        dmm_sum.update((dmm_sum.value() == null ? BigDecimal.valueOf(0) : dmm_sum.value()).add(value.getDmm()));

                        trex = tr_sum.value().divide(BigDecimal.valueOf(DMI_START_FLAG), SCALE, RoundingMode.HALF_UP);
                        dmmex = dmm_sum.value().divide(BigDecimal.valueOf(DMI_START_FLAG), SCALE, RoundingMode.HALF_UP);
                        dmpex = dmp_sum.value().divide(BigDecimal.valueOf(DMI_START_FLAG), SCALE, RoundingMode.HALF_UP);
                    } else {
                        trex = value.getTr().multiply(BigDecimal.valueOf(2)).add(tr_ex.value().multiply(BigDecimal.valueOf(13))).divide(BigDecimal.valueOf(15), SCALE, RoundingMode.HALF_UP);
                        dmpex = value.getDmp().multiply(BigDecimal.valueOf(2)).add(dmp_ex.value().multiply(BigDecimal.valueOf(13))).divide(BigDecimal.valueOf(15), SCALE, RoundingMode.HALF_UP);
                        dmmex = value.getDmm().multiply(BigDecimal.valueOf(2)).add(dmm_ex.value().multiply(BigDecimal.valueOf(13))).divide(BigDecimal.valueOf(15), SCALE, RoundingMode.HALF_UP);
                    }
                    tr_ex.update(trex);
                    dmp_ex.update(dmpex);
                    dmm_ex.update(dmmex);

                    if (trex.doubleValue() != 0) {
                        BigDecimal pdi = dmpex.multiply(BigDecimal.valueOf(100)).divide(trex, SCALE, RoundingMode.HALF_UP);
                        BigDecimal mdi = dmmex.multiply(BigDecimal.valueOf(100)).divide(trex, SCALE, RoundingMode.HALF_UP);
                        stockMid.setPdi(pdi);
                        stockMid.setMdi(mdi);
                        if (!mdi.add(pdi).equals(BigDecimal.ZERO)) {
                            mpdi = mdi.subtract(pdi).abs().divide(pdi.add(mdi).doubleValue() == 0 ? BigDecimal.ONE : pdi.add(mdi), SCALE, RoundingMode.HALF_UP).multiply(BigDecimal.valueOf(100));
                        } else {
                            mpdi = BigDecimal.ZERO;
                        }
                    }
                    if (value.getRk() <= ADX_START_FLAG) {
                        adx.update((adx.value() == null ? BigDecimal.valueOf(0) : adx.value()).add(mpdi));

                        if (value.getRk() == ADX_START_FLAG) {
                            BigDecimal adx_first = adx.value().divide(BigDecimal.valueOf(6), SCALE, RoundingMode.HALF_UP);
                            adx.update(adx_first);
                            stockMid.setAdx(adx_first);
                        }
                    } else {
                        BigDecimal last_adx = mpdi.multiply(BigDecimal.valueOf(2)).add(adx.value().multiply(BigDecimal.valueOf(5))).divide(BigDecimal.valueOf(7), SCALE, RoundingMode.HALF_UP);
                        adx.update(last_adx);
                        stockMid.setAdx(last_adx);
                    }
                }
                return stockMid;
            }
        });
        result.print();

        result.addSink(JdbcSink.sink(
                "INSERT INTO ods_stock_step_two " +
                        "VALUES" +
                        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.getName());
                    ps.setInt(2, t.getRk());
                    ps.setString(3, t.getCode());
                    ps.setString(4, t.getDate());
                    ps.setBigDecimal(5, t.getDeal_amount());
                    ps.setBigDecimal(6, t.getClosing_price());
                    ps.setBigDecimal(7, t.getEma12());
                    ps.setBigDecimal(8, t.getEma26());
                    ps.setBigDecimal(9, t.getDif());
                    ps.setBigDecimal(10, t.getClosing_diff());
                    ps.setBigDecimal(11, t.getLast_closing());
                    ps.setBigDecimal(12, t.getObv());
                    ps.setBigDecimal(13, t.getRsv());
                    ps.setBigDecimal(14, t.getUp6());
                    ps.setBigDecimal(15, t.getDown6());
                    ps.setBigDecimal(16, t.getUp12());
                    ps.setBigDecimal(17, t.getDown12());
                    ps.setBigDecimal(18, t.getUp24());
                    ps.setBigDecimal(19, t.getDown24());
                    ps.setBigDecimal(20, t.getRsi6());
                    ps.setBigDecimal(21, t.getRsi12());
                    ps.setBigDecimal(22, t.getRsi24());
                    ps.setBigDecimal(23, t.getK());
                    ps.setBigDecimal(24, t.getD());
                    ps.setBigDecimal(25, t.getJ());
                    ps.setBigDecimal(26, t.getSar());
                    ps.setBigDecimal(27, t.getDea());
                    ps.setBigDecimal(28, t.getMacd());
                    ps.setBigDecimal(29, t.getPdi());
                    ps.setBigDecimal(30, t.getMdi());
                    ps.setBigDecimal(31, t.getAdx());

                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1).withMaxRetries(10) //这里批次大小来提交，这里最好写1次，因为我们处理的是历史数据
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://127.0.0.1:3306/spider_base?useSSL=false")
                        .withUsername("root")
                        .withPassword("root")
                        .withDriverName(Driver.class.getName())
                        .build()
        ));


        env.execute();
    }
}
