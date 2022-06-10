package com.holden.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-source_file_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月20日19:41 - 周六
 * @Describe 从文件中读取
 */
public class source_file_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*
        * 1.若参数是目录,就会读取目录下所有文件
        * 2.若参数时文件,就只读取文件
        * 3.路径是相对也可以是绝对,相对路径是以project根目录,standalone模式下是集群节点根目录
        * 4.可以直接从hdfs上读取
        * */
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        DataStream<String> source = env.readTextFile(inputPath);

        //打印输出
        source.print();
        env.execute();
    }
}


































