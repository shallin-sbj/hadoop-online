package com.scl.online.kafka.service;

import com.alibaba.fastjson.JSON;
import com.scl.online.kafka.domain.Message;
import com.scl.online.kafka.domain.TopicConst;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaReceiver {

    @KafkaListener(topics = {TopicConst.EXECUTOR_TOPIC})
    public void listen(String message) {
        log.info("------------------接收消息 listen message =" + message);
        Message msg = JSON.parseObject(message, Message.class);
        log.info("MessageConsumer: onMessage: message is: [" + msg + "]");
        log.info("------------------ message =" + message);

    }
}
