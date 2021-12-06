package com.atguigu.state;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName FlinkDemo-FaultTolerance
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月26日20:05 - 周五
 * @Describe 状态后端的配置
 */
public class FaultTolerance_4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //旧版的RocksDBStateBackend 等价用法
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt\\checkpoint-dir");

        //启用checkpoint，5是进行checkpoint的间隔，单位毫秒，这里是300ms进行一次checkpoint
        env.enableCheckpointing(300, CheckpointingMode.EXACTLY_ONCE);

        //设置checkpoint的超时时间,这里规定了检查点必须在1分钟内完成，否则会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //允许两个连续的checkpoint错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        //指的是前一次checkpoint结束到后一次checkpoint开始之间的时间不能小于这个，就是规定了两个checkpoint之间的间隔不能小于100毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100L);

        //重启策略的配置
        env.setRestartStrategy(RestartStrategies.fallBackRestart());//回滚重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));//每隔10秒重启一次，尝试三次
        //超过10分钟最多尝试3次重启，每次间隔一分钟
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));


        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });


        env.execute();
    }
}
