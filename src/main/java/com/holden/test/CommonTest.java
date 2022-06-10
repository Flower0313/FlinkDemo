package com.holden.test;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName FlinkDemo-random_test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日13:40 - 周三
 * @Describe
 */
public class CommonTest {
    public static void main(String[] args) {
        Map<String, String> test = new HashMap<>();
        test.put("1", "flower");
        test.put("1", "holden");
        System.out.println("长度:" + test.size());
        System.out.println(test.get("1"));
    }
}
