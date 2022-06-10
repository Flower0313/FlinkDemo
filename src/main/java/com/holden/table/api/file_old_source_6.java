package com.holden.table.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-file_source_6
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月06日14:34 - 周一
 * @Describe 老版本的file_source方式
 */
public class file_old_source_6 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        //step-1 表的元数据,这里顺序要一致
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("timeStamp", DataTypes.BIGINT())
                .field("temperature", DataTypes.FLOAT());

        //step-2 连接文件，并创建一个临时表，其实就是一个动态表
        tEnv.connect(new FileSystem()
                        .path("T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt"))
                .withSchema(schema)//定义表结构
                /*
                * explain
                *  fieldDelimiter是一行数据根据逗号划分字段
                *  lineDelimiter每行数据根据换行符划分一条数据
                * */
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .createTemporaryTable("sensor");//创建临时表


        //step-3 做成表对象,然后对动态表进行查询
        Table resTable = tEnv.from("sensor")
                .groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));
        tEnv.toRetractStream(resTable, Row.class).print("cnt");

        env.execute();

    }
}
