package com.holden.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName FlinkDemo-OrderEvent
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日18:01 - 周三
 * @Describe 订单事件
 */


@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {
    //订单Id
    private Long orderId;
    //事件类型,<创建订单,支付订单,...>
    private String eventType;

    //交易码
    private String txId;
    private Long eventTime;
}
