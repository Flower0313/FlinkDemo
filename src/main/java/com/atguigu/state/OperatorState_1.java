package com.atguigu.state;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName FlinkDemo-OperatorState_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月25日1:25 - 周四
 * @Describe 算子状态
 */
public class OperatorState_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSink<Tuple2<String, Long>> dataStream = env.fromElements(
                        Tuple2.of("a", 3L),
                        Tuple2.of("a", 5L),
                        Tuple2.of("b", 7L),
                        Tuple2.of("c", 4L),
                        Tuple2.of("a", 13L),
                        Tuple2.of("c", 2L))
                .addSink(new BufferingSink(6));

        env.execute("OperatorState");
    }

    static class BufferingSink implements SinkFunction<Tuple2<String, Long>>, CheckpointedFunction {

        /*
         * 状态数据不参与序列化
         * 托管状态是由Flink框架管理的State,如ValueState、ListState、MapState
         * 其序列化和反序列化由Flink框架提供支持，无须用户感知、干预
         * */
        private final int threshold; //写出临界值
        private transient ListState<Tuple2<String, Long>> checkpointedState;
        private List<Tuple2<String, Long>> bufferedElements = new ArrayList<>();

        public BufferingSink(int threshold) {
            this.threshold = threshold;
        }


        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //每个并行任务都会执行且只执行一次
            System.out.println("initializeState");
            //分发器
            ListStateDescriptor<Tuple2<String, Long>> descriptor =
                    new ListStateDescriptor<Tuple2<String, Long>>("bufferedSinkState",//别名
                            TypeInformation.of(new TypeHint<Tuple2<String, Long>>() { //TypeInformation是类型消息
                            }));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {//第一次执行程序是不会进来的
                for (Tuple2<String, Long> element : checkpointedState.get()) {
                    bufferedElements.add(element);
                    System.out.println("initializeState >>" + element);
                }
            }

        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            for (Tuple2<String, Long> element : bufferedElements) {
                checkpointedState.add(element);
            }
            System.out.println("snapshotState");
        }


        @Override
        public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
            //每次元素进来都会调用一次
            bufferedElements.add(value);
            System.out.println("invoke>>> " + value);
            if(bufferedElements.size() == threshold){
                //达到写出临界值再统一写出
                for (Tuple2<String, Long> element : bufferedElements) {
                    //用户的业务逻辑，写出数据到外部存储,我这里直接打印
                    System.out.println(Thread.currentThread().getId() + " >> " + element.f0 + " : " + element.f1);
                }
                bufferedElements.clear();//输出完后清空缓存
            }

        }
    }
}
