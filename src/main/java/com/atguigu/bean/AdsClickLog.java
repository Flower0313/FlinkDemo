package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName FlinkDemo-AdsClickLog
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日14:06 - 周三
 * @Describe
 */


@Data
@AllArgsConstructor
@NoArgsConstructor
public class AdsClickLog {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}
