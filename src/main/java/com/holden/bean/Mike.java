package com.holden.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @ClassName FlinkDemo-Mike
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年7月26日22:15 - 周二
 * @Describe
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Mike {
    private String code;
    private String name;
    private String ds;
    private int rk;
    private BigDecimal STOR;
    @Builder.Default
    private BigDecimal MIDR = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal WEKR = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal WEKS = BigDecimal.ZERO;
    @Builder.Default
    private BigDecimal MIDS = BigDecimal.ZERO;
    private BigDecimal STOS;
}
