package com.holden.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @ClassName FlinkDemo-OdsStock
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月13日14:30 - 周一
 * @Describe
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OdsStock {
    private String code;
    private String name;
    private BigDecimal closing_price;
    private BigDecimal last_closing;
    private String date;
    private BigDecimal deal_amount;
    private int rk;
    private BigDecimal x;
    private BigDecimal i;
    private BigDecimal rsv;
    private BigDecimal highest;
    private BigDecimal lowest;
    private String table;
}
