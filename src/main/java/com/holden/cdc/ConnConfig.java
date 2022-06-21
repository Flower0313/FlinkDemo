package com.holden.cdc;

/**
 * @ClassName FlinkDemo-ConnConfig
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月10日10:56 - 周五
 * @Describe
 */
public class ConnConfig {
    public final static String DATABASE = "spider_base";
    public final static String TABLE_LIST = "spider_base.employee,spider_base.department";
    //精度
    public final static Integer SCALE = 6;
    //初始值
    public final static String INITIAL = "0.0";
    //分隔符
    public final static String delimiter = "-";

    public final static Integer REMOVE_FLAG = 3;
}
