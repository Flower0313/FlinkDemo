package com.holden.cdc;

import java.math.BigDecimal;

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
    public final static Integer SCALE = 8;
    //初始值
    public final static BigDecimal INITIAL = new BigDecimal("0.0");
    //分隔符
    public final static String delimiter = "-";

    public final static Integer REMOVE_FLAG = 3;

    public final static BigDecimal SAR_AF = new BigDecimal("0.02");

    public final static Integer FIRST_DAY = 1;

    //SAR指标开始天数
    public final static Integer SAR_START_FLAG = 4;

    //DMI指标开始天数
    public final static Integer DMI_START_FLAG = 14;

    public final static Integer ADX_START_FLAG = 19;

    public final static BigDecimal EMA3MU = BigDecimal.valueOf(4);

    public final static BigDecimal EMA3ZI = EMA3MU.subtract(BigDecimal.valueOf(2));


}
