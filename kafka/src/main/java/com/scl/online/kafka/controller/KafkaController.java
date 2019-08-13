package com.scl.online.kafka.controller;

import com.scl.online.kafka.service.KafkaSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka")
public class KafkaController {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private KafkaSender sender;

    @RequestMapping(value = "/send", method = RequestMethod.GET)
    public String sendKafka(String message) {
        try {
            logger.info("sendKafka 的消息={}", message);
            sender.send(message);
            logger.info("发送sendKafka成功.");
            return "SUCCESS";
        } catch (Exception e) {
            logger.error("发送kafka失败", e);
            return "FAIL";
        }
    }
}
