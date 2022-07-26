package com.holden.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @ClassName FlinkDemo-PreMike
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年7月26日21:35 - 周二
 * @Describe
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class PreMike {
    private String code;
    private String name;
    private String ds;
    private int rk;
    private BigDecimal hhv;
    private BigDecimal llv;
    private BigDecimal highest;
    private BigDecimal lowest;
    private BigDecimal hlc;
}
