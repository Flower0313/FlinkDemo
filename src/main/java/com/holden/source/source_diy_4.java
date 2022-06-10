package com.holden.source;

import com.holden.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @ClassName FlinkDemo-source_diy_4
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月20日21:35 - 周六
 * @Describe 自定义数据源
 */
public class source_diy_4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> source = env.addSource(new MySensorSource());

        source.print("diy-source");

        env.execute();
    }


}

//实现数据源泛型类,若要实现多并行度方法就实现ParallelSourceFunction这个方法
class MySensorSource implements SourceFunction<SensorReading> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {
        //定义一个随机数发生器
        Random random = new Random();

        //设置10个传感器初始温度
        HashMap<String, Double> sensorTempMap = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            //生成随机温度
            sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
        }

        while (isRunning) {
            for (String sId : sensorTempMap.keySet()) {
                //在当前温度基础上随机波动
                Double newTemp = sensorTempMap.get(sId) + random.nextGaussian();
                sensorTempMap.put(sId, newTemp);
                ctx.collect(new SensorReading(sId, System.currentTimeMillis(), newTemp));
            }
            //一秒钟更新一次
            Thread.sleep(2000L);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

