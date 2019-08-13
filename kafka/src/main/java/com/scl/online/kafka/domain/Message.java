package com.scl.online.kafka.domain;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.Date;

@Data
public class Message {
    private Long id;    //id

    private int code;  //返回码

    private String msg; //消息

    private Date startTime;  //时间戳

    private Date sendTime;  //时间戳

    private String logPath; //日志地址

    public static void main(String[] args) {
        Message message = new Message();
        message.setCode(200);
        message.setId(100L);
        message.setLogPath("/apps/logs");
        message.setSendTime(new Date());
        message.setMsg("test kafka");
        message.setStartTime(new Date());
        String jsonString = JSONObject.toJSONString(message);
        System.out.println(jsonString);
        Message message1 = JSONObject.parseObject(jsonString, Message.class);
    }
}
