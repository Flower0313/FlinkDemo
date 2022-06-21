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
                    .build();
        }).keyBy(OdsStock::getCode);





        env.execute();
    }
}
