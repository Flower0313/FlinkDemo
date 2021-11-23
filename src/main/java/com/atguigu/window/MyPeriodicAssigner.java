package com.atguigu.window;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @ClassName FlinkDemo-MyPeriodicAssigner
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月23日14:07 - 周二
 * @Describe
 */
public class MyPeriodicAssigner implements AssignerWithPeriodicWatermarks {

    private Long bound = 60 * 1000L; //延迟一分钟
    private Long maxTs = Long.MIN_VALUE; //当前最大时间戳


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return null;
    }

    @Override
    public long extractTimestamp(Object element, long recordTimestamp) {
        return 0;
    }
}
