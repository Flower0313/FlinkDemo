package com.holden.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName FlinkDemo-TxEvent
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日18:02 - 周三
 * @Describe 订单支付事件
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {
    //交易Id
    private String txId;
    //付款渠道
    private String payChannel;
    //交易时间
    private Long eventTime;
}
