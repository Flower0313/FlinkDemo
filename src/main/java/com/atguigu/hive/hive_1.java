package com.atguigu.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @ClassName FlinkDemo-hive_1
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月07日23:04 - 周二
 * @Describe
 */
public class hive_1 {
    public static void main(String[] args) {
        /*System.setProperty("HADOOP_USER_NAME", "holdenxiao");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String name = "myhive";  // Catalog 名字
        String defaultDatabase = "atguigu"; // 默认数据库
        // hive配置文件的目录. 需要把hive-site.xml添加到该目录
        String hiveConfDir = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\";

        // 1. 创建HiveCatalog
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        // 2. 注册HiveCatalog
        tEnv.registerCatalog(name, hive);
        // 3. 把 HiveCatalog: myhive 作为当前session的catalog
        tEnv.useCatalog(name);
        tEnv.useDatabase("atguigu");

        //指定SQL语法为Hive语法
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tEnv.sqlQuery("select * from action").execute().print();*/

    }
}
