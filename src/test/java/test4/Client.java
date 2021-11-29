package test4;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;

/**
 * @ClassName FlinkDemo-Client
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月25日20:10 - 周四
 * @Describe
 */
public class Client {
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        //使用匿名内部类可以获取到Person方法
        //因为抽象类的实现只有实现这个抽象类或匿名重写父类，总之父类还是这个抽象类
        //这里就把Student转换为匿名内部类，其本质就是抽象类，因为只有接口和抽象类能有匿名类
        Student<Tuple> stu1 = new Student<Tuple>("flower") {};
        System.out.println("stu1:" + stu1);

        //普通的类则不能
        //Student<Person> stu2 = new Student<Person>("xiao");
        //System.out.println("stu2:" + stu2);



        //匿名内部类
        Action<Person> action = new Action<Person>() {
            @Override
            public void eat() {
                System.out.println("xixi");
            }
        };
    }

}


class Person<E> {
    public Person() {
        System.out.println("person");
    }

    public String name = "person";
}


class Student<T> extends Person<String>{
    private String name;

    public Student(String name) throws NoSuchFieldException, IllegalAccessException {
        //如何获取Student中的T
        this.name = name;
        System.out.println("Student的类型:"+this.getClass());
        System.out.println("Student的父类:"+this.getClass().getSuperclass().getTypeName());
        System.out.println("Student的泛型:"+this.getClass().getGenericSuperclass().getTypeName());
    }
}

interface Action<T>{
    void eat();
}

