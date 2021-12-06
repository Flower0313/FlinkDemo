package com.atguigu.trans;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-trans_connect_8
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月04日19:37 - 周六
 * @Describe
 */
public class trans_connect_8 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//并行度1，只开放一个slot

        DataStreamSource<Integer> intStream = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<String> stringStream = env.fromElements("a", "b", "c");

        // 把两个流连接在一起: 貌合神离
        /*
        * 1.两个流的存储数据类型可以不同
        * 2.只是机械的合并在一起,内部仍然是分离的2个流
        * 3.只能2个流进行connect,不能有第3个
        * */
        ConnectedStreams<Integer, String> cs = intStream.connect(stringStream);
        cs.getFirstInput().print("first");
        cs.getSecondInput().print("second");


        env.execute();
    }

}
