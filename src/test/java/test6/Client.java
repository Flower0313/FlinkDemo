package test6;

/**
 * @ClassName FlinkDemo-Client
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日15:26 - 周二
 * @Describe
 */
public class Client {
    public static void main(String[] args) {
    }
}

interface AA{
    void show();
}

class BB implements AA{

    @Override
    public void show() {
        System.out.println("我是bb");
    }
}

