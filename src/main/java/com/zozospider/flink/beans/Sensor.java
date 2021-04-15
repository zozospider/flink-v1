package com.zozospider.flink.beans;

// 传感器温度读数的数据类型
public class Sensor {

    // id
    private String id;
    // 时间戳
    private Long time;
    // 温度
    private Double temp;

    public Sensor() {
    }

    public Sensor(String id, Long time, Double temp) {
        this.id = id;
        this.time = time;
        this.temp = temp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Double getTemp() {
        return temp;
    }

    public void setTemp(Double temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "Sensor{" +
                "id='" + id + '\'' +
                ", time=" + time +
                ", temp=" + temp +
                '}';
    }

}
