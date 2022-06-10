package com.holden.test;

/**
 * @ClassName FlinkDemo-GenericTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月09日13:52 - 周四
 * @Describe
 */


public class GenericDemo {
    public static void main(String[] args) {
        //调用GenericTest静态方法,泛型为String
//        WaterTest<String> show = GenericTest.<String>show();
//        System.out.println(show.name = "dd");

        NiuPi.<String>show("313");
    }
}

class WaterTest<R> {
    public R name;
}

class GenericTest<T> {
    //<E>是声明了泛型方法
    //WaterTest<E>是返回值类型,这里是WaterTest<E>,这里的E要和上面的<E>一样
    //show是方法名
    static <E> WaterTest<E> show() {
        return new WaterTest<>();
    }
}

class NiuPi {
    static <T> void show(T t) {
        System.out.println(t);
    }
}


