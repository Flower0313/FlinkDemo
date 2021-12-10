package com.atguigu.table.time.groupwindows;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @ClassName FlinkDemo-table_window_3
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日13:39 - 周二
 * @Describe 滑动窗口
 * 官方文档:https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/table/streaming/time_attributes.html
 */
public class table_process_window_3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // step-1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // step-2. 将DataStream -> Table
        Table sensorTable = tableEnv.fromDataStream(dataStream, $("id"), $("timeStamp"), $("temperature"), $("rt").proctime());

        //attention 过时的开窗方法
        //sensorTable.window(Tumble.over("10.seconds").on("rt").as("tw"));

        //attention 推荐的开窗方法
        /*
         * explain
         *  10秒一个窗口，时间戳是本地时间戳，和你字段中的时间戳没有关系，每个窗口中的数据都会分组聚合然后输出
         * */
        Table resTable = sensorTable.window(Tumble.over(
                                lit(10).seconds()) //10秒的滑动窗口
                        .on($("rt")) //根据rt的时间戳分窗口
                        .as("tw")) //别名
                .groupBy($("id"), $("tw"))
                .select($("id"), $("id").count().as("cnt"), $("temperature").avg().as("tAvg"), $("tw").end());


        /*
         * Q&A!
         * Q1:为什么这里明明有聚合操作,要用到以前的数据,为什么能用AppendStream呢?
         * A1:因为聚合是在每一个窗口中进行的，而不是来一个就输出到流中，而是在窗口中完成聚合才将一个结果输出
         *    到流中，这就直接追加就行了;相当于每个窗口只输出一个流结果,界限就是窗口,下个窗口用不到上个窗口
         *    的数据了。
         *    你可以可以使用toRetractStream试验一下,会显示每条数据都是true,因为没有撤回的消息
         * */
        tableEnv.toAppendStream(resTable, Row.class).print();
        //resTable.execute().print();

        env.execute();
    }
}
