package test4;

/**
 * @ClassName FlinkDemo-Client
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月25日20:10 - 周四
 * @Describe
 */
public class Client {
    public static void main(String[] args) {
        Student stu1 = new Student(){};
        Student stu2 = new Student();
        System.gc();
        System.out.println(stu1.getClass());
        System.out.println(stu2.getClass());
    }

}


class Person{
    public Person() {
        System.out.println("person");
    }

    public String name = showName();

    private String showName() {
        System.out.println("flower");
        return "flower";
    }


}

class Student extends Person{
    public Student() {
        System.out.println("student");
    }
}
