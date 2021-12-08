package com.atguigu.state;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @ClassName FlinkDemo-OperatorState_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月05日18:05 - 周日
 * @Describe
 */
public class OperatorState_5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "input/sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.map(new MyCounterMapper()).print();


        env.execute();
    }

    public static class MyCounterMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {
        //使用本地变量充当变量,但只要发生故障,这个存在内存中的变量就消失了,也不能进行故障恢复
        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }

        /**
         * 对状态进行快照,使之保存在CheckPoint中
         *
         * @param checkpointId
         * @param timestamp    时间戳
         * @return 状态列表
         * @throws Exception
         */
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            //这个需求中就将单值封装成List集合保存在CheckPoint中
            return Collections.singletonList(count);
        }

        /**
         * 当发生故障恢复后,从已经保存的CheckPoint中的状态来恢复之前的状态
         *
         * @param state
         * @throws Exception
         */
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            //这个需求之中每个List<Integer>其实就只有一个单值
            for (Integer integer : state) {
                count += integer;
            }
        }
    }
}
