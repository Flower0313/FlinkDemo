package com.atguigu.function;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @ClassName FlinkDemo-udf_scalar_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日17:29 - 周二
 * @Describe 自定义标量函数, 实现求id的hash值
 * 官方文档:https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/functions/udfs/
 */
public class udf_scalar_1 {
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

        //step-6 调用自定义函数
        //sub-step-6.1 在环境中注册UDF
        tEnv.createTemporarySystemFunction("fHashcode", new HashCode(23));

        //sub-step-6.2 Table API
        Table apiTable = sensorTable.select($("id"), call("fHashcode", $("id")));

        //sub-step-6.3 SQL
        tEnv.createTemporaryView("sensor", sensorTable);
        Table sqlTable = tEnv.sqlQuery("select id,fHashcode(id) from sensor");

        //step-7 使用不注册的函数,注意这里需要定义空参构造器用来反射
        Table noRegister = sensorTable.select(call(HashCode.class, $("id")));

        //step-8 打印输出
        apiTable.execute().print();
        sqlTable.execute().print();
        noRegister.execute().print();
        env.execute();
    }

    //step-5 自定义标量函数,实现求id的hash值
    public static class HashCode extends ScalarFunction {
        private int factor = 313;

        //attention 用来给java反射的
        public HashCode() {
        }

        public HashCode(int factor) {
            this.factor = factor;
        }

        //attention 必须要定义的方法
        public int eval(String str) {
            //explain 将传入进来的值的hash值乘自己的方法
            return str.hashCode() * factor;
        }
    }
}
