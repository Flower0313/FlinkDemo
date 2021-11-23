package gener;

/**
 * @ClassName FlinkDemo-testDemo
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月19日16:40 - 周五
 * @Describe
 */
public class testDemo {
    public static void main(String[] args) {
        /*Worker worker = new Worker();
        worker.<String>show(new Student2<Integer>()).test2("haha");*/
        Student2 stu1= new Student2("nn");
        Student2 stu2= new Student2("nn"){};
        System.out.println(stu1.getClass());
        System.out.println(stu2.getClass());

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

    public void test2(T1 t1) {
        System.out.println("T1的类型:" + t1.getClass());
    }
    public Student2(String name){
        System.out.println("niu");
    }
}

class Worker {
    //限定类型擦除到String
    <T extends String> Student2<T> show(Object obj) {
        return (Student2<T>) obj;
    }
}

