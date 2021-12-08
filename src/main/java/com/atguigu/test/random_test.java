package com.atguigu.test;

import java.util.Random;

/**
 * @ClassName FlinkDemo-random_test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日13:40 - 周三
 * @Describe
 */
public class random_test {
    public static void main(String[] args) {
        Random ran = new Random();
        System.out.println(ran.nextInt(3000));
    }
}
