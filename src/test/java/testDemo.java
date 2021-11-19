/**
 * @ClassName FlinkDemo-testDemo
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月19日16:40 - 周五
 * @Describe
 */
public class testDemo {
    public static void main(String[] args) {
        Student1 stu1 = new Student1();
        stu1.test("flower");

        Student2 stu2 = new Student2();
        stu2.test2("xiaohua");
        stu2.test2(313);
    }
}

abstract class Person<T1, T2> {
    T1 age;

    abstract public void test(T2 name);
}

class Student1<T1, T2> extends Person<T1, T2> {
    @Override
    public void test(T2 name) {
        System.out.println("Student1:" + name);
    }
}

class Student2<T1> extends Person<T1, Integer> {
    @Override
    public void test(Integer name) {
        System.out.println("Student2:" + name);
    }

    public void test2(T1 t1){
        System.out.println(t1);
    }
}

