package com.holden.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName FlinkDemo-UserBehavior
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日1:19 - 周三
 * @Describe 用户行为类
 */

/*
 * Explain
 *
 * @Data 提供类所有属性的 getting 和 setting 方法，此外还提供了equals、canEqual、hashCode、toString方法
 * @NoArgsConstructor 注解在类上；为类提供一个无参的构造方法
 * @AllArgsConstructor 注解在类上；为类提供一个全参的构造方法
* */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserBehavior {
    private Long userId;//用户id
    private Long itemId;//物品id
    private Integer categoryId;//分类id
    private String behavior;//用户行为
    private Long timestamp;//时间戳
}
