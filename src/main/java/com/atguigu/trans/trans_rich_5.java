package com.atguigu.trans;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

import javax.xml.crypto.Data;
import java.util.Map;

/**
 * @ClassName FlinkDemo-trans_rich_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日16:47 - 周日
 * @Describe 富类型
 */
public class trans_rich_5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple2<String, String>> resMap = dataStream.map(new RichMap());

        resMap.print();
        env.execute();
    }
}

//普通函数
class NormalMap implements MapFunction<SensorReading, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(SensorReading value) throws Exception {
        return new Tuple2<>(value.getId(), value.getId().length());//返回的参数类型决定R，调用map方法的对象决定T
    }
}

//富函数
class RichMap extends RichMapFunction<SensorReading, Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> map(SensorReading value) throws Exception {
        //可以获取subtask执行的序号
        return new Tuple2<>(value.getId(), "subtask-" + getRuntimeContext().getIndexOfThisSubtask());
    }

    /**
     * 初始化工作，一般可以用来连接数据库，不然在map中就不用来一次连接一次
     * 每个并行度分区都执行一次
     * 若setParallelism(3)那就是执行3次
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("连接成功！");
    }

    @Override
    public void close() throws Exception {
        System.out.println("关闭连接!");
    }
}














