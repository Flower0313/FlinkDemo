package com.holden.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-BroadcastState_10
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月09日21:43 - 周四
 * @Describe
 */
public class BroadcastState_10 {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);//广播流会将数据广播到每个分区,每个分区都能访问到

        //313流
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 31313);

        //88流
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop102", 8888);

        //Step-2 定义广播状态(可以定义多个状态)
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("state1", String.class, String.class);
        MapStateDescriptor<String, String> stateDescriptor2 = new MapStateDescriptor<>("state2", String.class, String.class);
        //将88流升级为广播流
        BroadcastStream<String> broadcastStream = controlStream.broadcast(stateDescriptor, stateDescriptor2);

        //Step-3 将313主流与88广播流连接
        dataStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, String, Object>() {
                    @Override//31313端口的值进来
                    public void processElement(String value, BroadcastProcessFunction<String, String, Object>.ReadOnlyContext ctx, Collector<Object> out) throws Exception {
                        //从ctx获取广播状态中取值,不同的值做不同的业务,可以看到这里只有ReadOnly
                        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                        //ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor2);
                        System.out.println(state.get("switch"));
                        System.out.println(state.get("ps4"));
                        System.out.println(state.get("xbox"));
                        if ("1".equals(state.get("switch"))) {
                            out.collect("切换到1号配置...");
                        } else if ("0".equals(state.get("switch"))) {
                            out.collect("切换到0号配置...");
                        } else {
                            out.collect("切换到其他配置...");
                        }
                    }

                    @Override//8888端口的值进来,也就是connect()方法中的流
                    public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, Object>.Context ctx, Collector<Object> out) throws Exception {
                        //提取状态,这个方法中的ctx中就可以修改广播状态
                        BroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                        //把值放入广播状态,这个值就是来自8888端口中的数据,这里将key值固定死了,当然可以根据你的需求改变,可以将流改成元组类型
                        //广播流可以增加多个值
                        state.put("switch", value);
                        state.put("ps4", "ps4sss");
                        state.put("xbox", "xboxsss");
                    }
                }).print("");

        env.execute();

    }
}
