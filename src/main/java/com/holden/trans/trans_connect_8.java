package com.holden.trans;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @ClassName FlinkDemo-trans_connect_8
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月04日19:37 - 周六
 * @Describe
 */
public class trans_connect_8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);//并行度1，只开放一个slot

        DataStreamSource<String> intStream = env.fromElements("1", "2", "3", "4", "b");
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c", "1");

        // 把两个流连接在一起: 貌合神离
        /*
         * 1.两个流的存储数据类型可以不同
         * 2.只是机械的合并在一起,内部仍然是分离的2个流
         * 3.只能2个流进行connect,不能有第3个
         * 4.谁调用谁就是主流,也就是泛型中的参数T,而在connect()参数中的流就是泛型参数R
         * 5.每个分区中的流都是轮询输出的,比如分区1中的流输出顺序是:A->B->A->B->A...可以设置并行度为1来查看
         * */
        ConnectedStreams<String, String> cs = intStream.connect(stringStream);

        cs.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String value) throws Exception {
                return "A流:" + value;
            }

            @Override
            public String map2(String value) throws Exception {
                return "B流:" + value;
            }
        }).print("");

        env.execute();
    }

}
