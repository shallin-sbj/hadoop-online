package com.scl.online.hadoop.hdfs.domain;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

@Data
public class User {
    private String name;
    private String age;


    public static void main(String[] args) {
        User user = new User();
        user.setAge("hello");
        user.setName("world");
        String toJSONString = JSONObject.toJSONString(user);
        System.out.println(toJSONString);

    }
}
