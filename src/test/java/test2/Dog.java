package test2;

/**
 * @ClassName FlinkDemo-Dog
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月24日14:23 - 周三
 * @Describe
 */
public class Dog<T> implements Tag{
    public String name;

    public Dog(String name) {
        this.name = name;
    }

    public void shut(){
        System.out.println("hello");
    }
}
