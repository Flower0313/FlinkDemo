package com.holden.cdc;

import com.alibaba.fastjson.JSONObject;
import com.holden.bean.SensorReading;
import com.mysql.cj.jdbc.Driver;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-sqlTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月08日10:44 - 周三
 * @Describe 若有需求就使用这种
 */
public class sqlTest {
    public static void main(String[] args) throws Exception {
        //注册流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //注册表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //连接数据源
        DebeziumSourceFunction<String> mysql = MySqlSource.<String>builder()
                .hostname("127.0.0.1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList(ConnConfig.DATABASE)
                .tableList(ConnConfig.TABLE_LIST)
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyCustomerDeserialization())
                .build();
        //转为Source
        DataStreamSource<String> mysqlDS = env.addSource(mysql);

        OutputTag<Department> departmentTag = new OutputTag<Department>("department") {
        };
        OutputTag<Employee> employeeTag = new OutputTag<Employee>("employee") {
        };

        //分流
        SingleOutputStreamOperator<Object> mainDataStream = mysqlDS.process(new ProcessFunction<String, Object>() {
            @Override
            public void processElement(String value, ProcessFunction<String, Object>.Context ctx, Collector<Object> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                String table = jsonObject.getString("table");
                JSONObject after = jsonObject.getJSONObject("after");
                if ("employee".equals(table)) {
                    Integer id = after.getInteger("id");
                    String name = after.getString("name");
                    Integer age = after.getInteger("age");
                    Integer dept_id = after.getInteger("dept_id");
                    ctx.output(employeeTag, new Employee(id, name, age, dept_id, Long.parseLong(after.getString("create_time"))));
                } else if ("department".equals(table)) {
                    Department department = new Department(after.getInteger("id"), after.getString("name"), after.getLong("create_time"));
                    ctx.output(departmentTag, department);
                }
            }
        });
        //Employee表
        DataStream<Employee> employeeStream = mainDataStream.getSideOutput(employeeTag).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Employee>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((element, recordTimestamp) -> element.getCreate_time()));

        //Department表
        DataStream<Department> departmentStream = mainDataStream.getSideOutput(departmentTag).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Department>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((element, recordTimestamp) -> element.getCreate_time()));

        //POJO中需要定义空参构造器
        Table empTable = tEnv.fromDataStream(employeeStream, $("id"), $("name"), $("age"), $("dept_id"), $("create_time").rowtime());

        Table depTable = tEnv.fromDataStream(departmentStream, $("id"), $("name"), $("create_time").rowtime());
        tEnv.createTemporaryView("employee", empTable);
        tEnv.createTemporaryView("department", depTable);

        /*
         * 两个表不能连接在一起开窗，只能单独先开窗再join，要开窗的话必须要使用datastream版的sql
         * 或者一个使用datastream的sql一个直接sql，也能写，但是直接sql的不能开窗
         * */

        Table table = tEnv.sqlQuery("select b.name,count(distinct a.name) as num " +
                "from employee a left join department b on a.dept_id=b.id " +
                "group by b.name");
        //Schema schema = Schema.newBuilder().column("name", DataTypes.STRING()).column("num", DataTypes.BIGINT()).build();

        DataStream<Row> rowDataStream = tEnv.toChangelogStream(table);

        SingleOutputStreamOperator<Row> resultStream = rowDataStream.filter(x -> {
            RowKind kind = x.getKind();
            //过滤掉null和撤回流操作
            return (RowKind.INSERT == kind || RowKind.UPDATE_AFTER == kind) && x.getField("name") != null && x.getField("num") != null;
        });



        //mysql幂等性写入
        resultStream.addSink(JdbcSink.sink(
                "INSERT INTO result " +
                        "VALUES" +
                        "(?,?) ON DUPLICATE KEY UPDATE name=?" +
                        ",num=?",
                (ps, t) -> {
                    ps.setString(1, Objects.requireNonNull(t.getField(0)).toString());
                    ps.setDouble(2, Double.parseDouble(Objects.requireNonNull(t.getField(1)).toString()));
                    ps.setString(3, Objects.requireNonNull(t.getField(0)).toString());
                    ps.setDouble(4, Double.parseDouble(Objects.requireNonNull(t.getField(1)).toString()));
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
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
