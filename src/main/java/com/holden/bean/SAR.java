package com.holden.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @ClassName FlinkDemo-SAY
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月21日18:58 - 周二
 * @Describe
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SAR {

    private BigDecimal sar;
    private BigDecimal af;
    private BigDecimal sar_high;
    private BigDecimal sar_low;
}
