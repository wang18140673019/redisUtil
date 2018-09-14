package com.hand.redis.model;/**
 * Created by 18771 on 2018/9/14.
 */


import java.io.Serializable;

/**
 * @author: ziming.wang@hand-china.com
 * @date: 2018/9/14  16:51
 * @desc:
 **/
public class  User implements Serializable {
    private String name;
    private Long   age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getAge() {
        return age;
    }

    public void setAge(Long age) {
        this.age = age;
    }
}
