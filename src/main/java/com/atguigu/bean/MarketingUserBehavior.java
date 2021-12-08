package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName FlinkDemo-MarketingUserBehavior
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日11:53 - 周三
 * @Describe 市场用户行为
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel; //访问渠道,一般有手机、电脑
    private Long timestamp;
}
