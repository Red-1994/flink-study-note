package com.red.flink.bean;

/**
 * <b>一句话简述此类的用途</b><br>
 *
 * <p>[详细描述]</p>
 * <p>
 * Date: 2022/7/12 16:56<br><br>
 *
 * @author 31528
 * @version 1.0
 */
public class StudentPojo {
    public int age;
    public String name;
    public StudentPojo(){

    }
    public StudentPojo(int age, String name) {
        this.age = age;
        this.name = name;
    }

    @Override
    public String toString() {
        return "StudentPojo{" +
                "age=" + age +
                ", name='" + name + '\'' +
                '}';
    }
}
