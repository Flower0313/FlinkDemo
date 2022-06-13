package com.holden.cdc;

import javafx.scene.layout.BackgroundImage;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @ClassName FlinkDemo-ddd
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月13日16:59 - 周一
 * @Describe
 */
public class ddd {
    public static void main(String[] args) {
        double a = -1123412.33412;
        double b = -55969966.235511;

        BigDecimal aa = BigDecimal.valueOf(3000.00);
        BigDecimal bb = BigDecimal.valueOf(3110.000000);
        System.out.println(bb.divide(BigDecimal.valueOf(0),2,RoundingMode.HALF_UP));
    }
}
