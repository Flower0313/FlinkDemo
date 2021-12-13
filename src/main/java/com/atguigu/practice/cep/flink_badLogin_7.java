package com.atguigu.practice.cep;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.atguigu.common.CommonEnv.LOGIN_SOURCE;

/**
 * @ClassName FlinkDemo-cep_practice_9
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月11日10:39 - 周六
 * @Describe 恶意登录监控
 */
public class flink_badLogin_7 {
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
                        WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner((e, r) -> e.getEventTime())
                ).keyBy(LoginEvent::getUserId);


        //Step-3 定义模式
        Pattern<LoginEvent, LoginEvent> fail = Pattern.<LoginEvent>begin("fail")//定义失败模式组
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .timesOrMore(2)//寻找登陆失败2次或以上的用户,忽略中间的其他值
                .consecutive()//在上面的条件上增加连续登陆的条件,也就是不忽视中间的其他值,必须严格连续
                .within(Time.seconds(2));//连续的数据集中的时间差不能超过2秒
        /*
         * Explain 上面代码就是查找出登陆失败的用户中中2秒内连续登陆超过2次及更多的数据
         * */

        //Step-4 注册模式
        PatternStream<LoginEvent> failPattern = CEP.pattern(dataSource, fail);

        //Step-5 获取结果
        failPattern
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                        //获取Map中的value
                        return pattern.get("fail").toString();
                    }
                }).print();
        env.execute();
    }
}
