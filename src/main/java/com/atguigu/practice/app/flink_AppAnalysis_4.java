package com.atguigu.practice.app;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @ClassName FlinkDemo-flink_AppAnalysis_4
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日11:56 - 周三
 * @Describe App市场推广统计
 */
public class flink_AppAnalysis_4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //todo 分渠道版
        env.addSource(new AppMarketingDataSource())
                .map(behavior -> Tuple2.of(behavior.getChannel() + "_" + behavior.getBehavior(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print("channel_App");

        //todo 不分渠道版
        env.addSource(new AppMarketingDataSource())
                .map(behavior -> Tuple2.of(behavior.getBehavior(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print("NoChannel_App");

        env.execute();
    }


    /**
     * Explain 自定义Source源
     * 泛型参数代表输出元素类型
     */
    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();

        //explain 渠道列表
        List<String> channels = Arrays.asList("huawei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");

        //explain 行为列表
        List<String> behaviors = Arrays.asList("download", "install", "update");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior behavior = new MarketingUserBehavior((long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());

                ctx.collect(behavior);
                Thread.sleep(random.nextInt(2000));
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
