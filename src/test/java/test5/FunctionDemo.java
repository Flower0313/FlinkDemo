package test5;

/**
 * @ClassName FlinkDemo-FunctionDemo
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月11日1:58 - 周六
 * @Describe
 */
public class FunctionDemo {
    public static void main(String[] args) {
        CC.shut(new BB() {
            @Override
            public void show() {
                System.out.println("卧槽尼玛");
            }
        });
    }
}

class CC {
    static void shut(BB bb){
        bb.show();
    }
}

interface AA{
    void show();
}

interface BB extends AA{

}
