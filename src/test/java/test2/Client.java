package test2;

import java.lang.reflect.Field;

/**
 * @ClassName FlinkDemo-Client
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月24日14:29 - 周三
 * @Describe
 */
public class Client {
    public String name = "client";

    public static void main(String[] args) {
        Dog<String> x1 = new Dog<>("x1");
        Dog<String> x2 = new Dog<String>("x2") {};//反射失败


        Field[] name = x1.getClass().getDeclaredFields();
        for (Field field : name) {
            System.out.println(field);
        }


    }
}
