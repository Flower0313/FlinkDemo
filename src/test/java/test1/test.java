package test1;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName FlinkDemo-test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月23日17:38 - 周二
 * @Describe
 */
public class test {
    public static void main(String[] args) {
        Map<String, String> diyMap = new HashMap<>();
        diyMap.put("1002", "flower");
        diyMap.put("1003", "holden");
        /*for (Map.Entry<String, String> entry : diyMap.entrySet()) {
            System.out.println("key=" + entry.getKey() + ",value=" + entry.getValue());
        }*/
        System.out.println(diyMap.toString());
        System.out.println((7*6*5*4*3*2)/((5*4*3*2)*2));
    }
}
