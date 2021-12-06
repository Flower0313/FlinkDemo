package test4;

import org.apache.flink.api.java.tuple.Tuple;

/**
 * @ClassName FlinkDemo-Client
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月25日20:10 - 周四
 * @Describe 1.getClass()是返回运行时的Class, 而不是定义时候的Class
 * 2.getSuperClass()是返回这个实体的超类Class对象,若这个Class是Object、接口、原始类型、空类型就返回null,若父类是接口,返回的是Object,这也是类只能单继承的表现
 * 3.getGenericSuperclass()返回此对象的超类的Type,这个Type就包含超类的泛型
 */
public class Client {
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        //这里就把Student转换为匿名内部类，其本质就是抽象类，因为只有接口和抽象类能有匿名类
        Student<Tuple> stu1 = new Student<Tuple>("flower") {
        };
        //System.out.println("stu1:" + stu1.getClass());//Client$1
        //System.out.println("stu1.parent:" + stu1.getClass().getSuperclass());//Student
        //使用匿名内部类的方式就获取到了Student<?>的泛型Tuple
        //System.out.println("stu2.parent:" + stu1.getClass().getGenericSuperclass());//Tuple



        //普通的类则不能
        //Student<Tuple> stu2 = new Student<Tuple>("xiao");
        //System.out.println("stu2:" + stu2.getClass());//Student
        //System.out.println("stu2.parent:" + stu2.getClass().getGenericSuperclass().getTypeName());//Person<T>


        //匿名内部类
        ActionA<Tuple> action = new ActionA<Tuple>() {
        };
        System.out.println(action.getClass());//Client$2
        System.out.println(action.getClass().getSuperclass());//父类是ActionA
        //所以这里获取的也是父类也就是ActionA的泛型类型
        System.out.println("action:" + action.getClass().getGenericSuperclass());//Tuple

        ActionB<Tuple> actionB = new ActionB<Tuple>() {
        };
        System.out.println(actionB.getClass());//Client$3
        System.out.println(actionB.getClass().getSuperclass());//ActionB
        System.out.println("action:" + actionB.getClass().getGenericSuperclass());//Tuple

        /*
         * Q&A
         * Q1:为什么匿名类getSuperclass()是其本身呢?
         * A1:你使用匿名类就相当于产生了一个虚空的子类，比如上面中的action,其实是一个虚空ActionA的子类,若不理解
         *    什么是虚空子类，那就看看下面的Runnable接口,它明明是一个接口,为什么还能有实现类呢,我在这里并没有声明
         *    它的子类,所以这就是匿名类的概念,只有这样理解才明白为什么action的getSuperclass是ActionA而不是A<T>,
         *    为什么actionB的getSuperclass是ActionB而不是B<T>
         *
         * Conclusion
         * 所以 OutputTag<SensorReading> lateTag = new OutputTag<SensorReading>("late"){};
         * 使用匿名内部类的方式,这样传过去反射就能解析到SensorReading这个类
         * */

        Runnable run1 = new Runnable() {
            @Override
            public void run() {

            }
        };
        System.out.println(run1.getClass().getSuperclass());//因为Runnable是接口,所以会显示Object

    }

}


class Person<T> {
    public Person() {
        System.out.println("person");
    }

    public String name = "person";
}


class Student<T> extends Person<T> {
    private String name;

    public Student(String name) throws NoSuchFieldException, IllegalAccessException {
        //如何获取Student中的T
        this.name = name;
        /*System.out.println(name + "的类型:" + this.getClass());
        System.out.println(name + "的父类:" + this.getClass().getSuperclass().getTypeName());
        System.out.println(name + "的泛型:" + this.getClass().getGenericSuperclass().getTypeName());*/
    }
}

interface A<T> {

}

class B<T> {

}

class ActionA<T> implements A<T> {

}


class ActionB<T> extends B<T> {

}