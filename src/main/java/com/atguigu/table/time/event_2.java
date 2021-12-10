package com.atguigu.table.time;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

import java.time.Duration;

/**
 * @ClassName FlinkDemo-event_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日1:33 - 周二
 * @Describe 定义事件时间戳
 */
public class event_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SensorReading> dataStream =
                env.fromElements(new SensorReading("sensor_1", 1000L, 10D),
                        new SensorReading("sensor_1", 2000L, 20D),
                        new SensorReading("sensor_2", 3000L, 30D),
                        new SensorReading("sensor_1", 4000L, 40D),
                        new SensorReading("sensor_1", 5000L, 50D),
                        new SensorReading("sensor_2", 6000L, 60D));

        // step-1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //step-2. 将DataStream转换为Table时,定义event时间特性
        //todo 方式一,字段中调用了rowtime就需要定义事件时间,可以调用.printSchema()查询字段的属性
        DataStream<SensorReading> ds = dataStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override//抽取出SensorReading类中的timeStamp字段作为水位线时间戳
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimeStamp() * 1000L;
                    }
                }));

        //todo lambda表达式写法
        dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(((element, recordTimestamp) -> element.getTimeStamp() * 1000L)));

        //用生成的水位线字段覆盖timeStamp，若不想覆盖可以另外定义字段
        Table table = tableEnv.fromDataStream(ds, "id,timeStamp.rowtime,temperature as temp");
        //Table table = tableEnv.fromDataStream(dataStream,$("id"),$("timeStamp"),$("temperature"),$("dt").rowtime());


        //todo 方式二,WaterMark语句在一个已有字段上定义一个watermark生成表达式，同时标记这个已有字段为时间属性字段
        String sourceDDL =
                "create table sensor_source(id STRING not null," +
                        "`timeStamp` BIGINT," +
                        "temperature DOUBLE" +
                        "rt as TO_TIMESTAMP(FROM_UNIXTIME(temperature))," +
                        "WATERMARK FOR rt as rt-INTERVAL '1' second" + //用延迟1秒的策略来生成水位线,second是别名
                        ") with (" +
                        "'connector'='filesystem'," +
                        "'format'='csv'," +
                        "'path'='T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt'" +
                        ")";


        //todo 方式三
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("timeStamp", DataTypes.BIGINT())
                .rowtime(new Rowtime().timestampsFromField("timestamp") //从字段中提取时间戳
                        .watermarksPeriodicBounded(1000))//watermark延迟1秒
                .field("temperature", DataTypes.DOUBLE());


        env.execute();
    }
}
