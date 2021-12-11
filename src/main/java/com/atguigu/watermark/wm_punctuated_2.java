package com.atguigu.watermark;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @ClassName FlinkDemo-wm_punctuated
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月11日9:24 - 周六
 * @Describe
 */
public class wm_punctuated_2 {
    public static void main(String[] args) {
        /*
         * 省略
         *
         * */
    }

    public static class MyPunctuatedWm implements WatermarkGenerator<SensorReading> {
        //最大时间戳
        private Long maxTs;
        //最大延迟时间
        private Long maxDelay;

        public MyPunctuatedWm(Long maxDelay) {
            this.maxDelay = maxDelay;
            this.maxTs = Long.MIN_VALUE + this.maxDelay + 1;
        }


        @Override
        public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
            //获取当前数据中最大的时间戳并赋值
            System.out.println("生成Watermark");
            maxTs = Math.max(eventTimestamp, maxTs);
            output.emitWatermark(new Watermark(maxTs - maxDelay - 1));

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

        }
    }
}
