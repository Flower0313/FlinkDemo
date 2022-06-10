package com.holden.cdc;

/**
 * @ClassName FlinkDemo-Department
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月09日18:46 - 周四
 * @Describe
 */
public class Department {
    private int id;
    private String name;
    private Long create_time;

    @Override
    public String toString() {
        return "Department{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", create_time=" + create_time +
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

    public Long getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Long create_time) {
        this.create_time = create_time;
    }

    public Department() {
    }

    public Department(int id, String name, Long create_time) {
        this.id = id;
        this.name = name;
        this.create_time = create_time;
    }
}
