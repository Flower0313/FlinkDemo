package test1;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.math.*;


/**
 * @ClassName FlinkDemo-test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月23日17:38 - 周二
 * @Describe
 */
public class test {
    public static void main(String[] args) {
        BigDecimal bigDecimal = new BigDecimal(0).equals(new BigDecimal(0)) ? new BigDecimal(1) : new BigDecimal(2);
        BigDecimal divide = new BigDecimal(0).divide(new BigDecimal(1), 4, RoundingMode.HALF_UP);
        System.out.println(bigDecimal);

    }
}
