package com.holden.cdc;

import com.alibaba.fastjson.JSONObject;
import com.holden.bean.OdsStock;
import com.holden.bean.SensorReading;
import com.holden.bean.StockMid;
import com.mysql.cj.jdbc.Driver;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.core.appender.db.jdbc.JdbcAppender;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static com.holden.common.CommonEnv.JDBC;
import static com.holden.common.CommonEnv.SQL_PASSWORD;

/**
 * @ClassName FlinkDemo-ddd
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月13日16:59 - 周一
 * @Describe
 */
public class ddd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        AtomicInteger atomicInteger = new AtomicInteger(1);

        DebeziumSourceFunction<String> mysql = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("spider_base")
                .tableList("spider_base.ods_stock_online")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyCustomerDeserialization())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(mysql);

        //转为POJO类并且keyBy分区
        KeyedStream<OdsStock, String> keyedStream = mysqlDS.map(x -> {
            JSONObject jsonObject = JSONObject.parseObject(x);
            JSONObject data = jsonObject.getJSONObject("data");
            return new OdsStock(data.getString("code"), data.getString("name"), data.getBigDecimal("closing_price"), data.getBigDecimal("last_closing"), data.getString("date"), data.getBigDecimal("deal_amount"), data.getInteger("rk"), data.getBigDecimal("x"), data.getBigDecimal("i"), data.getBigDecimal("rsv"), data.getBigDecimal("highest"), data.getBigDecimal("lowest"), jsonObject.getString("table"));
        }).keyBy(OdsStock::getCode);


        SingleOutputStreamOperator<StockMid> result = keyedStream.map(new RichMapFunction<OdsStock, StockMid>() {
            //map状态
            private MapState<String, StockMid> stockState;

            @Override
            public void open(Configuration parameters) throws Exception {
                stockState = getRuntimeContext().getMapState(new MapStateDescriptor<String, StockMid>("my-map", String.class, StockMid.class));
            }

            @Override
            public void close() throws Exception {
                stockState.clear();
                System.out.println("结束了:" + atomicInteger);
            }

            @Override
            public StockMid map(OdsStock value) throws Exception {
                atomicInteger.incrementAndGet();
                StockMid stockMid = new StockMid();
                String key = value.getCode() + "-" + value.getRk();
                //第一条数据
                if (value.getRk() == 1) {
                    BigDecimal highest = value.getHighest();
                    BigDecimal lowest = value.getLowest();
                    BeanUtils.copyProperties(stockMid, value);
                    stockMid.setObv(new BigDecimal(ConnConfig.INITIAL));
                    stockMid.setLast_closing(new BigDecimal(ConnConfig.INITIAL));
                    stockMid.setDiff(new BigDecimal(ConnConfig.INITIAL));
                    stockMid.setEma12(value.getClosing_price());
                    stockMid.setEma26(value.getClosing_price());
                    stockMid.setClosing_diff(new BigDecimal(ConnConfig.INITIAL));

                    BigDecimal rsv = BigDecimal.valueOf(0.0);
                    if (!Objects.equals(highest, lowest)) {
                        rsv = (value.getClosing_price().subtract(lowest).multiply(BigDecimal.valueOf(100))).divide(highest.subtract(lowest), ConnConfig.SCALE, RoundingMode.HALF_UP);
                        stockMid.setRsv(rsv);
                    } else {
                        stockMid.setRsv(new BigDecimal(ConnConfig.INITIAL));
                    }
                    stockMid.setK(rsv);
                    stockMid.setD(rsv);
                    stockMid.setJ(rsv);
                    stockMid.setUp6(new BigDecimal(ConnConfig.INITIAL));
                    stockMid.setUp12(new BigDecimal(ConnConfig.INITIAL));
                    stockMid.setUp24(new BigDecimal(ConnConfig.INITIAL));
                    stockMid.setDown6(new BigDecimal(ConnConfig.INITIAL));
                    stockMid.setDown12(new BigDecimal(ConnConfig.INITIAL));
                    stockMid.setDown24(new BigDecimal(ConnConfig.INITIAL));

                    stockState.put(key, stockMid);
                } else {
                    //除开第一条后的数据
                    if (!stockState.isEmpty()) {
                        String last_key = value.getCode() + "-" + (value.getRk() - 1);
                        //手动移除掉前面的状态，不然后面的读取速度会变慢
                        if (value.getRk() >= 3) {
                            stockState.remove(value.getCode() + "-" + (value.getRk() - 2));
                        }
                        //取上一条记录
                        StockMid last_Stock = stockState.get(last_key);
                        BeanUtils.copyProperties(stockMid, value);
                        BigDecimal closing_diff = value.getClosing_price().subtract(last_Stock.getClosing_price());
                        BigDecimal ema12 = (BigDecimal.valueOf(2).multiply(value.getClosing_price()).add(BigDecimal.valueOf(11).multiply(last_Stock.getEma12()))).divide(BigDecimal.valueOf(13), ConnConfig.SCALE, RoundingMode.HALF_UP);
                        BigDecimal ema26 = (BigDecimal.valueOf(2).multiply(value.getClosing_price()).add(BigDecimal.valueOf(24).multiply(last_Stock.getEma26()))).divide(BigDecimal.valueOf(26), ConnConfig.SCALE, RoundingMode.HALF_UP);

                        stockMid.setRsv(value.getRsv());
                        stockMid.setEma12(ema12);
                        stockMid.setEma26(ema26);
                        stockMid.setDiff(ema12.subtract(ema26));
                        stockMid.setClosing_diff(closing_diff);
                        stockMid.setLast_closing(last_Stock.getClosing_price());
                        if (closing_diff.compareTo(BigDecimal.valueOf(0)) > 0) {
                            stockMid.setObv(last_Stock.getObv().add(value.getDeal_amount()));

                            stockMid.setUp6((closing_diff.add(last_Stock.getUp6().multiply(BigDecimal.valueOf(5)))).divide(BigDecimal.valueOf(6), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setUp12((closing_diff.add(last_Stock.getUp12().multiply(BigDecimal.valueOf(11)))).divide(BigDecimal.valueOf(12), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setUp24((closing_diff.add(last_Stock.getUp24().multiply(BigDecimal.valueOf(23)))).divide(BigDecimal.valueOf(24), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            ;
                            stockMid.setDown6(last_Stock.getDown6().abs().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setDown12(last_Stock.getDown12().abs().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setDown24(last_Stock.getDown24().abs().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), ConnConfig.SCALE, RoundingMode.HALF_UP));
                        } else if (closing_diff.compareTo(BigDecimal.valueOf(0)) < 0) {
                            stockMid.setObv(last_Stock.getObv().subtract(value.getDeal_amount()));
                            stockMid.setDown6((closing_diff.abs().add(last_Stock.getDown6().multiply(BigDecimal.valueOf(5)))).divide(BigDecimal.valueOf(6), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setDown12((closing_diff.abs().add(last_Stock.getDown12().multiply(BigDecimal.valueOf(11)))).divide(BigDecimal.valueOf(12), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setDown24((closing_diff.abs().add(last_Stock.getDown24().multiply(BigDecimal.valueOf(23)))).divide(BigDecimal.valueOf(24), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setUp6(last_Stock.getUp6().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setUp12(last_Stock.getUp12().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setUp24(last_Stock.getUp24().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), ConnConfig.SCALE, RoundingMode.HALF_UP));
                        } else {
                            stockMid.setObv(last_Stock.getObv());
                            stockMid.setDown6(last_Stock.getDown6().abs().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setDown12(last_Stock.getDown12().abs().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setDown24(last_Stock.getDown24().abs().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setUp6(last_Stock.getUp6().multiply(BigDecimal.valueOf(5)).divide(BigDecimal.valueOf(6), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setUp12(last_Stock.getUp12().multiply(BigDecimal.valueOf(11)).divide(BigDecimal.valueOf(12), ConnConfig.SCALE, RoundingMode.HALF_UP));
                            stockMid.setUp24(last_Stock.getUp24().multiply(BigDecimal.valueOf(23)).divide(BigDecimal.valueOf(24), ConnConfig.SCALE, RoundingMode.HALF_UP));

                        }
                        BigDecimal k = (value.getRsv().add(last_Stock.getK().multiply(BigDecimal.valueOf(2)))).divide(BigDecimal.valueOf(3), ConnConfig.SCALE, RoundingMode.HALF_UP);
                        BigDecimal d = (k.add(last_Stock.getD().multiply(BigDecimal.valueOf(2)))).divide(BigDecimal.valueOf(3), ConnConfig.SCALE, RoundingMode.HALF_UP);
                        stockMid.setK(k);
                        stockMid.setD(d);
                        stockMid.setJ(k.multiply(BigDecimal.valueOf(3)).subtract(d.multiply(BigDecimal.valueOf(2))));

                        stockState.put(key, stockMid);
                    }
                }
                return stockMid;
            }
        });
        result.print();

        result.addSink(JdbcSink.sink(
                "INSERT INTO ods_stock_result " +
                        "VALUES" +
                        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (ps, t) -> {
                    ps.setString(1, t.getName());
                    ps.setInt(2, t.getRk());
                    ps.setString(3, t.getCode());
                    ps.setString(4, t.getDate());
                    ps.setBigDecimal(5, t.getDeal_amount());
                    ps.setBigDecimal(6, t.getClosing_price());
                    ps.setBigDecimal(7, t.getEma12());
                    ps.setBigDecimal(8, t.getEma26());
                    ps.setBigDecimal(9, t.getDiff());
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
                    ps.setBigDecimal(20, t.getK());
                    ps.setBigDecimal(21, t.getD());
                    ps.setBigDecimal(22, t.getJ());
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1000)
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