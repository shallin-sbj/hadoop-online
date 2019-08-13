package com.scl.online.hadoop.hdfs.utils;

import com.scl.online.hadoop.hdfs.domain.Person;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SerializationUtilTest {
    public static void main(String[] args) throws Exception {
        //测试序列化
        Person person = new Person("zhangsan", 27, "man");
        byte[] values = HadoopSerializationUtils.serialize(person);

        //测试反序列化
        Person per = new Person();
        HadoopSerializationUtils.deserialize(per, values);
        System.out.println(per);
    }
}
