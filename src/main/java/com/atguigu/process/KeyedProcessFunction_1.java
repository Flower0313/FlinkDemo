package com.atguigu.process;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @ClassName FlinkDemo-KeyedProcessFunction_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月26日20:53 - 周五
 * @Describe
 */
public class KeyedProcessFunction_1 {
    public static void main(String[] args) throws Exception {
        //step-1 创建环境 & 读取数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 31313);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //step-2 逻辑操作
        dataStream.keyBy(SensorReading::getId)
                .process(new MyProcess()).print();


        //step-3 环境执行
        env.execute();
    }

    /*
     * Explain 泛型参数一是key(keyBy分组的那个key的类型)，参数二是输入类型，参数三是输出类型
     * 注意KeyedProcessFunction继承了AbstractRichFunction,所以富函数能做的事,Process都能做
     * */
    public static class MyProcess extends KeyedProcessFunction<String, SensorReading, Integer> {

        //step-1 定义单值状态
        private ValueState<Long> tsTimerState;


        //step-2 打开生命周期 & 并给flink注册状态值
        @Override
        public void open(Configuration parameters) throws Exception {
            //attention 注意ValueState对应ValueStateDescriptor的注册器
            tsTimerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-timer", Long.class));
        }

        //step-3(*) 每个元素进来后执行的方法
        @Override
        public void processElement(SensorReading value, KeyedProcessFunction<String,
                SensorReading, Integer>.Context ctx,
                                   Collector<Integer> out) throws Exception {
            //todo 执行逻辑开始

            System.out.println("-----------------------------------------");


            out.collect(value.getId().length());

            //Attention Context上下文能调用的对象
            //1.获取时间戳,本质就是获取元素的hasTimestamp
            //ctx.timestamp();

            //2.获取当前key,也就是keyBy中的那个key
            //ctx.getCurrentKey();

            //3.若你涉及分流操作,里面可以传入你的侧输出流标记
            //ctx.output(...);

            //4.获取当前处理(ProcessingTime)时间戳
            //ctx.timerService().currentProcessingTime();

            //5.若设置了事件时间(EventTime),获取当前的水位线
            //ctx.timerService().currentWatermark();

            //todo 定时器闹钟,只对应相同的key有效，所以可以使用KeyedState来保存当前的时间戳,因为只delete闹钟必须要和注册的时间戳相同
            //6.1注册处理时间闹钟,这样设置相当于当前时间1秒后执行闹钟操作,所以不同的定时器是根据传入的时间戳来区分的
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
            //6.2若你直接设置一个常量10000,那相当于在1970-01-01-00:10时触发
            //ctx.timerService().registerProcessingTimeTimer(1000L);
            //6.3删除处理时间闹钟,删除闹钟时传入的时间戳需要和指定闹钟时传入的时间戳相同才能删除
            //ctx.timerService().deleteProcessingTimeTimer(1000L);


            //7.1注册事件时间闹钟,相当于在你数据中的时间戳的1秒后调用
            //注意事件定时器的执行时间要再加1秒,因为事件时间根据水位线来触发,水位线是减去了1ms,所以5999需要6000的数据触发
            //ctx.timerService().registerEventTimeTimer(value.getTimeStamp() + 1000L);
            //7.2删除事件时间闹钟
            //ctx.timerService().deleteEventTimeTimer(value.getTimeStamp() + 1000L);

            /*
             * Example-1
             *  要求:删除处理时间定义的闹钟定时器
             *  方法:使用状态保存处理时间戳,因为处理时间戳是随着机器时间变化的,每条数据进来后得到的时间戳都不相同,
             *      所以我们需要一个状态值来保存当前的时间戳,好给后面的数据使用
             * */
            //long processTime = ctx.timerService().currentProcessingTime();//获取当前处理流时间戳
            //tsTimerState.update(processTime);//存入状态
            //ctx.timerService().registerProcessingTimeTimer(processTime + 1000L);//注册当前时间戳1秒后的闹钟
            //ctx.timerService().deleteProcessingTimeTimer(processTime + 1000L);//删除上面注册的闹钟

        }

        //todo 闹钟被执行需要做的事情
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, SensorReading, Integer>.OnTimerContext ctx, Collector<Integer> out) throws Exception {
            //ctx.output();也能做侧输出流
            //out.collect();也能将一些值输出到主流
            //ctx.timeDomain();判断当前是处理时间还是事件时间
            //Attention watermark的周期性生成其本质就是使用在这里使用套娃,在定时器执行的方法中再定义一个定时器
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1000L);
            System.out.println(ctx.getCurrentKey() + "闹钟" + timestamp + "响了！");
        }

        //todo 关闭的方法,清空状态值
        @Override
        public void close() throws Exception {
            tsTimerState.clear();
        }
    }


}

