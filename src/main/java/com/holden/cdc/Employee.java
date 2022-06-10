package com.holden.cdc;

import java.util.Objects;

/**
 * @ClassName FlinkDemo-Employee
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月09日11:24 - 周四
 * @Describe
 */
public class Employee {
    private int id;
    private String name;
    private int age;
    private int dept_id;
    private Long create_time;

    public Employee() {
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", dept_id=" + dept_id +
                ", create_time='" + create_time + '\'' +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getDept_id() {
        return dept_id;
    }

    public void setDept_id(int dept_id) {
        this.dept_id = dept_id;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Employee employee = (Employee) o;
        return id == employee.id && age == employee.age && dept_id == employee.dept_id && Objects.equals(name, employee.name) && Objects.equals(create_time, employee.create_time);
    }

    public Long getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Long create_time) {
        this.create_time = create_time;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, age, dept_id, create_time);
    }

    public Employee(int id, String name, int age, int dept_id, Long create_time) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.dept_id = dept_id;
        this.create_time = create_time;
    }
}
