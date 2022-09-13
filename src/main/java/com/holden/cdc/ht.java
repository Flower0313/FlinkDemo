package com.holden.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName FlinkDemo-ht
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年8月30日9:51 - 周二
 * @Describe
 */
public class ht {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //Long值使用BIGINT来替代
        //没有PROCTIME()属性字段的表都是维表，一般维表的是变化不频繁的
        //写出外部数据库也要指明唯一的key，能根据key值进行更改的，比如es和redis就有幂等性
        /*tEnv.executeSql("create table hb_dim_lead_custom_tag(" +
                "`lead_id` bigint," +
                "`custom_tag_id` string," +
                "rt timestamp_ltz(3) metadata from 'op_ts'," +
                "PRIMARY KEY (lead_id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='htdw_bi'," +
                "'table-name'='hb_dim_lead_custom_tag'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");

        tEnv.executeSql("create table hb_dim_lead_appoint(" +
                "`lead_id` bigint," +
                "`is_visit` int," +
                "`is_audition_signed` int," +
                "`is_appointed` int," +
                "rt timestamp_ltz(3) metadata from 'op_ts'," +
                "PRIMARY KEY (lead_id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='htdw_bi'," +
                "'table-name'='hb_dim_lead_appoint'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");

        tEnv.executeSql("create table dim_lead_data_contract(" +
                "`id` bigint," +
                "`contract_transform_num` bigint," +
                "`lead_transform_num` bigint," +
                "`lead_transform_gy_num` bigint," +
                "`lead_transform_ylk_num` bigint," +
                "`total_lesson_cnt` DECIMAL," +
                "`remainder_cnt` DECIMAL," +
                "rt timestamp_ltz(3) metadata from 'op_ts'," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='htdw_bi'," +
                "'table-name'='dim_lead_data_contract'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");*/

        tEnv.executeSql("create table hb_dim_lead_tag(" +
                "`lead_id` bigint," +
                "`tag_id` string," +
                "rt timestamp_ltz(3) metadata from 'op_ts'," +
                "WATERMARK FOR rt AS rt - interval '0' second," +
                "PRIMARY KEY (lead_id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='dw_bi'," +
                "'table-name'='hb_dim_lead_tag'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");


        tEnv.executeSql("create table crm_lead(" +
                "`id` bigint," +
                "`is_deleted` BOOLEAN," +
                "`status` int," +
                "`store_id` bigint," +
                "`create_time` TIMESTAMP," +
                "`last_update_time` TIMESTAMP," +
                "`holder_id` bigint," +
                "`lead_owner_id` bigint," +
                "`holder_dept_id` bigint," +
                "`lead_owner_dept_id` bigint," +
                "`tenant_id` bigint," +
                "`last_active_time` TIMESTAMP," +
                "`last_follow_status` int," +
                "`public_id` bigint," +
                "rt timestamp_ltz(3) metadata from 'op_ts'," +
                "WATERMARK FOR rt AS rt - interval '0' second," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='htdw_bi'," +
                "'table-name'='crm_lead'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");


        tEnv.sqlQuery("select a.id,b.tag_id from crm_lead as a left join hb_dim_lead_tag as b on a.id=b.lead_id").execute().print();
    }
}
