package com.atguigu.state;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-KeyedState_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月25日19:36 - 周四
 * @Describe 键控状态
 */
public class KeyedState_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.keyBy(SensorReading::getId)//分组
                .map(new MyKeyCountMapper());//先执行map中参数，再执行map


        env.execute("KeyedState");
    }

    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {
        /*
         * 每个keyBy分区中都有属于自己的State,只有对应的key才能获取
         * */

        //ValueState类型状态,单个值
        private ValueState<Integer> keyCountState;

        //ListState类型状态,一组数据的列表
        private ListState<String> myListState;
        //MapState类型状态,key-value对
        private MapState<String, Double> myMapState;
        //ReducingState类型状态,用于聚合操作的列表
        private ReducingState<SensorReading> myReducingState;

        @Override//每个分区执行一次，注意不是分组
        public void open(Configuration parameters) throws Exception {
            System.out.println("获取上下文环境");

            /*
             * Explain!!!
             * 1.注册状态值
             * 2.声明键控状态:参数一是State的名字，参数二是State中数据类型描述，用来序列化和反序列化
             * 3.放在open中调用是因为你放在外面的时候还不能直接getRuntimeContext,必须要生命周期open后,才能获取到getRuntimeContext
             * */
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));//可以设置初始值,不设置就默认为null
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("my-reduce", new ReduceFunction<SensorReading>() {
                @Override//聚合逻辑
                public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                    // Explain 由结果可知道,7条数据只聚合6次，那么就是每个按key分区的第一条数据不会触发这个方法
                    //System.out.println("value1:" + value1 + ",value2:" + value2);
                    return new SensorReading(value1.getId(), value2.getTimeStamp(), value1.getTemperature() + value2.getTemperature());
                }
            }, SensorReading.class));
        }


        @Override
        public void close() throws Exception {
            keyCountState.clear();
            myListState.clear();
            myMapState.clear();
            myReducingState.clear();
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            Integer count = 0;//初始个数
            if (keyCountState.value() != null) {
                count = keyCountState.value();
            }
            count++;
            keyCountState.update(count);//ValueState状态
            myListState.add(value.getTemperature().toString());//ListState状态，里面存的是key对应的温度值
            myMapState.put(value.getId(), value.getTemperature());
            myReducingState.add(value);//直接聚合

            //System.out.println(value.getId() + "的个数为" + count);
            //System.out.println("MapState:" + myMapState.get(value.getId()));
            //System.out.println("ReducingState" + myReducingState.get());

            //循环不同key中的状态列表
            for (String s : myListState.get()) {
                //System.out.println("ListState:" + s);
            }
            return count;
        }
    }
}

