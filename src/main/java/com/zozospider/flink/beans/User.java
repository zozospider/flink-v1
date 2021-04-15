package com.zozospider.flink.beans;

// name group age
// one A 100
// two A 10
// three B 20
// four A 1
// four B 200
// five C 30
public class User {

    String name;
    String group;
    Integer age;

    public User() {
    }

    public User(String group, Integer age) {
        this.group = group;
        this.age = age;
    }

    public User(String name, String group, Integer age) {
        this.name = name;
        this.group = group;
        this.age = age;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", group='" + group + '\'' +
                ", age=" + age +
                '}';
    }

}
