package test1;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

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
        String str = "{\"buy4\":\"9.640\",\"date\":\"2022-09-13\",\"buy5\":\"9.630\",\"sale2hand\":\"164700\",\"buy2\":\"9.660\",\"code\":\"sh600352\",\"buy3\":\"9.650\",\"buy1\":\"9.670\",\"sale1\":\"9.680\",\"buy4hand\":\"112800\",\"sale2\":\"9.690\",\"sale4hand\":\"250110\",\"buy2hand\":\"100300\",\"sale5\":\"9.720\",\"sale3\":\"9.700\",\"sale4\":\"9.710\",\"sale3hand\":\"70300\",\"buy1hand\":\"96081\",\"sale1hand\":\"110700\",\"name\":\"浙江龙盛\",\"sale5hand\":\"342300\",\"buy3hand\":\"123800\",\"time\":\"15:00:00\",\"buy5hand\":\"115300\"}";

        System.out.println(JSON.parse(str));

    }
}
