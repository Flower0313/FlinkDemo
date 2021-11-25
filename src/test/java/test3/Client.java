package test3;

/**
 * @ClassName FlinkDemo-Client
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月24日17:02 - 周三
 * @Describe 局部内部类
 */
public class Client {
    public static void main(String[] args) {
        Outer.outMethod1();
        Outer outer = new Outer();
        outer.outMethod2();
    }
}

class Outer {
    private static int a = 1;
    private int b = 2;

    /*
    * Q:为什么方法中给内部类使用的属性会默认加final呢？
    * A:因为我们调用的只是outMethod方法，它是存储在栈中的，调用完栈空间就释放了，但是它里面的类是存储在堆中的，
    *   所以还将outMethod方法中的属性声明为final将存储周期延长，这样内部类就能调用了，不用担心方法释放了。
    * */
    public static void outMethod1() {
        final int c = 3;
        class Inner {
            public void inMethod() {
                System.out.println("out.a = " + a);
                //错误的，因为outMethod是静态方法，不能直接调用非静态的属性
                //System.out.println("out.b = " + b);
                System.out.println("out.local.c = " + c);
            }
        }
        Inner in = new Inner();
        in.inMethod();
    }

    public void outMethod2(){
        int c = 3;//默认为final
        class Inner {
            public void inMethod() {
                System.out.println("out.a = " + a);
                System.out.println("out.b = " + b);//非静态内部类可以调用b
                System.out.println("out.local.c = " + c);
            }
        }
        Inner in = new Inner();
        System.out.println(in.getClass());//class test3.Outer$2Inner
        in.inMethod();
    }
}
