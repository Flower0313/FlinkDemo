package com.atguigu.wc;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-WordCount2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月19日17:53 - 周五
 * @Describe flink之流处理WordCount程序
 * com.atguigu.wc.WordCount2
 */
public class WordCount2 {
    public static void main(String[] args) throws Exception {
        //todo 流式
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop102", 31313);
        /*//设置参数为--host 192.168.10.102 --port 7777
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        String host = parameterTool.get("host"); //这个名称可以自定义，但是一定要和传入--后面的名字一样
        int port = parameterTool.getInt("port");

        DataStreamSource<String> source = env.socketTextStream(host, port);*/


        //String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\wc.txt";

        //todo 读取数据
        //DataSource中本质就是DataSet
        //DataStream<String> source = env.readTextFile(inputPath);

        source.flatMap(new WordCount1.MyFlatMapper())
                .keyBy(x -> x.f0).sum(1)//sum会将之前的数据也存在内存以便下次累积
                .print("wc");



       /* DataStream<Tuple2<String, Integer>> res =
                source.flatMap(new WordCount1.MyFlatMapper())
                        .keyBy(x -> x.f0)
                        //传参1表示数据在索引中的位置
                        .sum(1).setParallelism(1);//这个写死的2只针对sum这个任务
        res.print();//.setParallelism(1);//设置并行度*/
        //结果中输出的前面的编号就是线程的编号，可以理解为分区
        //默认并行度是你电脑的核数
        //可以看到它的输出结果是叠加的，hello逐渐从1到4，而不是像批处理那样直接输出4
        env.execute();
    }
}


