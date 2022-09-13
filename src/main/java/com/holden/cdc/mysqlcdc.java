package com.holden.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName FlinkDemo-mysqlcdc
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月08日15:30 - 周三
 * @Describe
 */
public class mysqlcdc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //Long值使用BIGINT来替代
        //没有PROCTIME()属性字段的表都是维表，一般维表的是变化不频繁的
        //写出外部数据库也要指明唯一的key，能根据key值进行更改的，比如es和redis就有幂等性
        tEnv.executeSql("create table contract_basic(" +
                "id BIGINT," +
                "lead_id BIGINT," +
                "business_id BIGINT," +
                "contract_status TINYINT," +
                "is_deleted BOOLEAN," +
                "create_time TIMESTAMP," +
                "update_time TIMESTAMP," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='htdw_bi'," +
                "'table-name'='contract_basic'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");


        tEnv.executeSql("create table contract_lesson_cnt_segment(" +
                "id BIGINT," +
                "contract_id BIGINT," +
                "lesson_cnt DECIMAL," +
                "lesson_cnt_type TINYINT," +
                "left_lesson_cnt DECIMAL," +
                "left_additional_lesson_cnt DECIMAL," +
                "is_deleted BOOLEAN," +
                "create_time TIMESTAMP," +
                "update_time TIMESTAMP," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='htdw_bi'," +
                "'table-name'='contract_lesson_cnt_segment'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");


        tEnv.executeSql("create table by_appointment(" +
                "id BIGINT," +
                "lead_id BIGINT," +
                "is_deleted BOOLEAN," +
                "appointment_time TIMESTAMP," +
                "is_visit BOOLEAN," +
                "is_audition_signed BOOLEAN," +
                "create_time TIMESTAMP," +
                "last_update_time TIMESTAMP," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='htdw_bi'," +
                "'table-name'='by_appointment'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");

        tEnv.executeSql("create table by_lead_tag(" +
                "id BIGINT," +
                "create_time TIMESTAMP," +
                "creator_id BIGINT," +
                "is_deleted BOOLEAN," +
                "last_update_id BIGINT," +
                "last_update_time TIMESTAMP," +
                "lead_id BIGINT," +
                "tag_id BIGINT," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='htdw_bi'," +
                "'table-name'='by_lead_tag'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");

        tEnv.executeSql("create table crm_lead_custom_tag(" +
                "id BIGINT," +
                "create_time TIMESTAMP," +
                "creator_id BIGINT," +
                "is_deleted BOOLEAN," +
                "last_update_id BIGINT," +
                "last_update_time TIMESTAMP," +
                "lead_id BIGINT," +
                "custom_tag_id BIGINT," +
                "PRIMARY KEY (id) NOT ENFORCED" +
                ") with (" +
                "'connector'='mysql-cdc'," +
                "'hostname'='47.98.109.82'," +
                "'port'='3306'," +
                "'username'='root'," +
                "'password'='Hutong_0709!!'," +
                "'database-name'='htdw_bi'," +
                "'table-name'='crm_lead_custom_tag'," +
                "'scan.startup.mode'='latest-offset'" +
                ")");

        //合同的剩余课时数(赠送课时+假期课时+假期膨胀课时+基础课时)
        Table leftHour = tEnv.sqlQuery("select contract_id,sum(left_lesson_cnt)+sum(left_additional_lesson_cnt) as remainder_cnt,sum(lesson_cnt) as total_cnt from contract_lesson_cnt_segment where is_deleted=false group by contract_id");

        tEnv.createTemporaryView("leftTable", leftHour);

        Table hb_dim_lead_contract = tEnv.sqlQuery("select obc.lead_id,COUNT(DISTINCT obc.id) contract_transform_num,COUNT(DISTINCT obc.id) lead_transform_num,COUNT(DISTINCT CASE WHEN (obc.business_id = 7 OR obc.business_id = 11) THEN obc.id ELSE NULL END) lead_transform_gy_num,COUNT(DISTINCT CASE WHEN (obc.business_id = 4 OR obc.business_id = 2) THEN obc.id ELSE NULL END) lead_transform_ylk_num,SUM(COALESCE(s3.total_cnt,0)) lesson_cnt,SUM(COALESCE(s3.remainder_cnt,0)) contract_remainder_cnt from contract_basic obc left join leftTable s3 on obc.id=s3.contract_id where obc.is_deleted=false and obc.contract_status not in (1,6) group by obc.lead_id");

        tEnv.createTemporaryView("hb_dim_lead_contract", hb_dim_lead_contract);

        Table dim_lead_data_contract = tEnv.sqlQuery("select lead_id,contract_transform_num,lead_transform_num,lead_transform_gy_num,lead_transform_ylk_num,lesson_cnt,contract_remainder_cnt,CASE WHEN lead_transform_num = lead_transform_gy_num AND lead_transform_gy_num > 0 THEN 1 ELSE 0 end is_only_gyk,CASE   WHEN lead_transform_num = lead_transform_ylk_num AND lead_transform_ylk_num > 0 THEN 1 ELSE 0 END is_only_ylk from hb_dim_lead_contract");

        tEnv.createTemporaryView("dim_lead_data_contract", dim_lead_data_contract);

        Table hb_dim_lead_appoint = tEnv.sqlQuery("SELECT  oba.lead_id,MAX(CASE WHEN oba.is_visit = TRUE THEN 1 WHEN oba.is_visit = FALSE THEN 0 ELSE -1 END) is_visit,MAX(CASE WHEN oba.is_audition_signed = TRUE THEN 1 WHEN oba.is_audition_signed = FALSE THEN 0 ELSE -1 END) is_audition_signed,CASE   WHEN DATE_FORMAT(TIMESTAMPADD(DAY,1,CURRENT_TIMESTAMP),'yyyyMMdd') <= DATE_FORMAT(MAX(oba.appointment_time),'yyyyMMdd') THEN 1 ELSE 0 END is_appointed FROM by_appointment oba WHERE oba.is_deleted = FALSE GROUP BY oba.lead_id");

        tEnv.createTemporaryView("hb_dim_lead_appoint", hb_dim_lead_appoint);

        //listagg，相当于聚合函数wm_concat()
        Table hb_dim_lead_tag = tEnv.sqlQuery("SELECT s1.lead_id,listagg (DISTINCT cast (s1.tag_id AS VARCHAR)) FROM by_lead_tag AS s1 WHERE s1.is_deleted = FALSE GROUP BY s1.lead_id");

        tEnv.createTemporaryView("hb_dim_lead_tag", hb_dim_lead_tag);

        tEnv.sqlQuery("select * from crm_lead_custom_tag").execute().print();

        //table.execute().print();
    }
}
