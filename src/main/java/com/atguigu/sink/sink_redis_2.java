package com.atguigu.sink;

import com.atguigu.bean.SensorReading;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import static com.atguigu.common.CommonEnv.PASSWORD;


/**
 * @ClassName FlinkDemo-sink_redis_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日19:45 - 周日
 * @Describe
 */
public class sink_redis_2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .setMaxTotal(100)
                .setTimeout(10000)
                .setPassword(PASSWORD)
                .build();

        dataStream.addSink(new RedisSink<>(redisConfig, new RedisMapper<SensorReading>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                //返回存在Redis中的数据类型，存储的是Hash，第二个参数指定hash的key
                return new RedisCommandDescription(RedisCommand.HSET, "sensor");
            }

            @Override
            public String getKeyFromData(SensorReading sensorReading) {
                //存入其中元素的key
                return sensorReading.getId();
            }

            @Override
            public String getValueFromData(SensorReading sensorReading) {
                //存入其中元素的value
                return JSON.toJSONString(sensorReading);
            }
        }));

        env.execute();
    }
}
