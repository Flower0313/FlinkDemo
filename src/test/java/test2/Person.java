package test2;


/**
 * @ClassName FlinkDemo-Person
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月24日14:22 - 周三
 * @Describe
 */
public class Person<T>{

    public Dog<T> dog;

    public Person<T> returnP(Dog<T> dog) {
        this.dog = dog;
        System.out.println(dog);
        return this;
    }

    public void showName(){
        System.out.println(this.dog.getClass());
    }

}

interface Tag{

}
