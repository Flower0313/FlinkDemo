package com.holden.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @ClassName FlinkDemo-StockMid
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月13日14:57 - 周一
 * @Describe
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StockMid {
    private String name;
    private int rk;
    private String code;
    private String date;
    private BigDecimal deal_amount;
    private BigDecimal closing_price;
    private BigDecimal ema12;
    private BigDecimal ema26;
    private BigDecimal diff;
    private BigDecimal closing_diff;
    private BigDecimal last_closing;
    private BigDecimal obv;
    private BigDecimal rsv;
    private BigDecimal up6;
    private BigDecimal down6;
    private BigDecimal up12;
    private BigDecimal down12;
    private BigDecimal up24;
    private BigDecimal down24;
    private BigDecimal k;
    private BigDecimal d;
    private BigDecimal j;
}
