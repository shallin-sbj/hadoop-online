package com.scl.online.kafka.kafkademo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by xujia on 2018/9/11
 */
public class KafkaProducter {
    private final KafkaProducer<String, String> producer;
    public final static String TOPIC = "itsm-test";
    private String services = "s201:9092,s202:9092,s203:9092,s204:9092";

    private KafkaProducter(){
        Properties properties = new Properties();
        //此处配置的是kafka的端口
//        properties.put("bootstrap.servers", "alservice.sulin.online:9092");
        properties.put("bootstrap.servers", services);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        properties.put("request.required.acks","-1");

        producer = new KafkaProducer<>(properties);
    }

    void produce() {
        //发送100条消息
        int messageNo = 10000000;
        int count = 200;
        while (messageNo < count) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            long startTime = System.currentTimeMillis();
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, data);
            producer.send(record, new DataCallback(startTime, data));
            System.out.println(data);
            messageNo++;
        }
    }
    public static void main( String[] args )
    {
        new KafkaProducter().produce();
    }
}