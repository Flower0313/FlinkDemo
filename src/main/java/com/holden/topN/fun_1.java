package com.holden.topN;

import com.holden.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName FlinkDemo-fun_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年2月17日0:27 - 周四
 * @Describe
 */
public class fun_1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //随便根据一个东西keyBy，因为ListState必须要用KeyedDataStream
        dataStream.keyBy(x -> 1).map(new MyFunction()).print("topN");


        env.execute();
    }

    public static class MyFunction extends RichMapFunction<SensorReading, List<SensorReading>> {
        private ListState<SensorReading> topList;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("注册排序状态");
            topList = getRuntimeContext().getListState(new ListStateDescriptor<SensorReading>("top", SensorReading.class));
        }

        @Override
        public void close() throws Exception {
            topList.clear();
        }

        @Override
        public List<SensorReading> map(SensorReading value) throws Exception {
            topList.add(value);

            List<SensorReading> sortTop = new ArrayList<>();
            for (SensorReading sensorReading : topList.get()) {
                sortTop.add(sensorReading);
            }

            sortTop.sort((x1, x2) -> -x1.getTemperature().compareTo(x2.getTemperature()));
            if (sortTop.size() > 3) {
                sortTop.remove(3);
            }
            topList.update(sortTop);

            return sortTop;
        }
    }
}
