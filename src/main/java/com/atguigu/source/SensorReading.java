package com.atguigu.source;

/**
 * @ClassName FlinkDemo-SensorReading
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月20日18:49 - 周六
 * @Describe 传感器温度读数类型
 */
public class SensorReading {
    private String id;
    private Long timeStamp;//时间戳
    private Double temperature;//温度

    public SensorReading(){}

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timeStamp=" + timeStamp +
                ", temperature=" + temperature +
                '}';
    }



    public SensorReading(String id, Long timeStamp, Double temperature) {
        this.id = id;
        this.timeStamp = timeStamp;
        this.temperature = temperature;
    }
}
