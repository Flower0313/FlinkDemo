package com.holden.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName FlinkDemo-LoginEvent
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月11日10:37 - 周六
 * @Describe 登陆事件类
 */


@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginEvent {
    private Long userId;//用户id
    private String ip;//ip
    private String eventType;//事件类型
    private Long eventTime;//事件时间
}
