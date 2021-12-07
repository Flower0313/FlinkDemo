package com.atguigu.function;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @ClassName FlinkDemo-udf_table_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日18:13 - 周二
 * @Describe 自定义表值函数,实现将id拆分，并输出(id,id.length)
 */
public class udf_table_2 {
    public static void main(String[] args) throws Exception {
        //step-1 定义环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //step-2 读取文件
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);

        //step-3 转换为POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //step-4 将流转换为表
        Table sensorTable = tEnv.fromDataStream(dataStream, $("id"), $("timeStamp").as("ts"), $("temperature").as("temp"));

        //step-5 注册自定义表函数，实现将id拆分，并输出(word,length)
        tEnv.createTemporarySystemFunction("SplitFunction", new Split(","));

        //step-6.1 过时的Table-API
        Table oldApiTable = sensorTable.joinLateral("SplitFunction(id) as (word,length)")
                .select("id,word,length");

        //step-6.2 推荐的Table-API
        Table newApiTable = sensorTable.joinLateral(
                //explain 给炸裂的id两个别名,因为collect出了两个元素,所以我们定义word和length变量
                call("SplitFunction", $("id")).as("word", "length"))
                .select($("id"), $("word"), $("length"));

        /*
        * step-6.3 推荐的sql
        * 在 SQL里面用JOIN或者以ON TRUE为条件的LEFT JOIN来配合 LATERAL TABLE(<TableFunction>) 的使用。
        * */
        tEnv.createTemporaryView("sensor", sensorTable);

        //attention 注意声明别名的方式
        Table sqlTable1 = tEnv.sqlQuery(
                "SELECT id, word, length " +
                        "FROM sensor,lateral table(SplitFunction(id)) as Flower(word,length)");

        Table sqlTable2 = tEnv.sqlQuery(
                "select id, word, length " +
                        "from sensor " +
                        "left join lateral table(SplitFunction(id)) as SplitFunction(word,length)" +
                        " on true");



        //sqlTable1.execute().print();
        //sqlTable2.execute().print();
        //oldApiTable.execute().print();
        //newApiTable.execute().print();
        env.execute();
    }

    //step-5 自定义标量函数,实现求id的hash值
    public static class Split extends TableFunction<Tuple2<String, Integer>> {
        //定义分隔符
        private String separatpr = "_";

        public Split(String separatpr) {
            this.separatpr = separatpr;
        }

        public void eval(String str) {
            for (String s : str.split(separatpr)) {
                collect(new Tuple2<String, Integer>(s, s.length()));
            }
        }
    }
}
