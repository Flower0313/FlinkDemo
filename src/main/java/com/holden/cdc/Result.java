package com.holden.cdc;

/**
 * @ClassName FlinkDemo-Result
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月09日17:41 - 周四
 * @Describe
 */
public class Result {
    private String name;
    private Double num;

    public Result() {
    }

    public Result(String name, Double num) {
        this.name = name;
        this.num = num;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getNum() {
        return num;
    }

    public void setNum(Double num) {
        this.num = num;
    }
}
