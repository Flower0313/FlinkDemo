package com.holden.practice.cep;

import com.holden.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;

import static com.holden.common.CommonEnv.LOGIN_SOURCE;

/**
 * @ClassName FlinkDemo-flink_badLogin_7_2
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月17日15:28 - 周五
 * @Describe 恶意登录监控
 */
public class flink_badLogin_7_2 {
    public static void main(String[] args) throws Exception {
        //Step-1 注册环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-2 准备数据 & 打上水位线时间戳 & 按UserId分区
        KeyedStream<LoginEvent, Long> dataSource = env.readTextFile(LOGIN_SOURCE)
                .map(ele -> {
                    String[] e = ele.split(",");
                    return new LoginEvent(Long.valueOf(e[0]),
                            e[1],
                            e[2],
                            Long.parseLong(e[3]) * 1000L);
                })//注册水位线时间戳
                .assignTimestampsAndWatermarks(
                        //水位线延迟20秒,容错可以有20秒的数据
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((e, r) -> e.getEventTime() * 1000L)
                ).keyBy(LoginEvent::getUserId);

        KeyedStream<LoginEvent, Long> keyedStream = dataSource.keyBy(LoginEvent::getUserId);

        SingleOutputStreamOperator<String> process = keyedStream
                .process(new LoginFailWarning2(2));

        process.print("");

        env.execute();
    }

    /**
     * 用定时器的缺点:时效性太慢,若黑客在定时器2秒内破译了上万次呢？因为此方法注册一个定时器,定时器会傻傻等2秒再执行
     */
    public static class LoginFailWarning1 extends KeyedProcessFunction<Long, LoginEvent, String> {
        //最大连续登陆失败次数
        private Integer maxFailTimes;
        //private List<LoginEvent> failLogins;

        public LoginFailWarning1(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        //定义失败状态列表
        private ListState<LoginEvent> failList;
        //保存定时器时间戳
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            failList = getRuntimeContext()
                    .getListState(new ListStateDescriptor<LoginEvent>("fail", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
        }

        @Override
        public void close() throws Exception {
            failList.clear();
            timerTsState.clear();
        }


        @Override
        public void processElement(LoginEvent value, KeyedProcessFunction<Long, LoginEvent, String>.Context ctx, Collector<String> out) throws Exception {
            //判断事件的类型
            if ("fail".equals(value.getEventType())) {
                //判断里面有没有定时器,没有的话就添加定时器
                if (timerTsState.value() == null) {
                    long ts = value.getEventTime() + 2000L;
                    ctx.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
                failList.add(value);
            } else {//若登陆成功清除状态 & 定时器
                if (timerTsState.value() != null) {
                    ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                    timerTsState.clear();
                    failList.clear();
                }
            }
        }


        //定时器触发
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, LoginEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发,说明至少2秒内没有登陆成功
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(failList.get());
            //若超过设定的警报值,就报警
            if (loginEvents.size() >= maxFailTimes) {
                out.collect(ctx.getCurrentKey().toString() + "," + loginEvents.get(0).getEventType());
            }
            failList.clear();
            timerTsState.clear();

        }
    }

    //时效性更好的方法,因为求2秒内失败登录次数超过2次,所以用这个方法简单
    public static class LoginFailWarning2 extends KeyedProcessFunction<Long, LoginEvent, String> {
        //最大连续登陆失败次数
        private Integer maxFailTimes;
        private ValueState<Long> timerTsState;
        private ListState<LoginEvent> failList;


        public LoginFailWarning2(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            failList = getRuntimeContext()
                    .getListState(new ListStateDescriptor<LoginEvent>("fail", LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
        }

        //以登陆事件为判断报警的触发条件，不用注册定时器
        @Override
        public void processElement(LoginEvent value, KeyedProcessFunction<Long, LoginEvent, String>.Context ctx, Collector<String> out) throws Exception {
            if ("fail".equals(value.getEventType())) {
                //1.登陆失败,
                Iterator<LoginEvent> iterator = failList.get().iterator();
                //若已经有登陆失败事件
                if (iterator.hasNext()) {
                    //继续判断是否在2秒内
                    LoginEvent lastLogin = iterator.next();
                    if (Math.abs(value.getEventTime() - lastLogin.getEventTime()) <= 2000) {
                        out.collect(ctx.getCurrentKey() + "," + lastLogin.getEventType());
                    }
                    failList.clear();
                    failList.add(lastLogin);
                } else {
                    failList.add(value);
                }
            } else {
                failList.clear();
            }
        }
    }
}
