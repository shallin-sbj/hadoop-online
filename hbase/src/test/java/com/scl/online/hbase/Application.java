package com.scl.online.hbase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * 测试HDFS的基本操作
 */
@RunWith(SpringRunner.class)
@ComponentScan("com.scl.online")
@SpringBootTest
public class Application {


    @Test
    public void contextLoads() {
    }

}
