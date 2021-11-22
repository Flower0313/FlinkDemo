package com.atguigu.source;

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

        //
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";
        DataStream<String> source = env.readTextFile(inputPath);

        //打印输出
        source.print();

        env.execute();
    }
}


































