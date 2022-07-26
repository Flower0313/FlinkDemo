package com.holden.source;

import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ClassName FlinkDemo-source_mysql_5
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年7月15日11:59 - 周五
 * @Describe
 */
public class source_mysql_5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);



        env.execute();
    }
}
