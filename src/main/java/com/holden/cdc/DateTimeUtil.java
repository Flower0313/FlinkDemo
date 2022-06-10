package com.holden.cdc;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @ClassName FlinkDemo-DateTimeUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月09日15:21 - 周四
 * @Describe
 */
public class DateTimeUtil {
    //自定义时间格式
    private final static DateTimeFormatter formatter
            = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formatter.format(localDateTime);

    }

    public static Long toTs(String YmDHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formatter);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

}
