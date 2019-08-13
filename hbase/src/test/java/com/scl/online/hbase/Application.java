package com.scl.online.hbase;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

/**
 * 测试HDFS的基本操作
 */
@RunWith(SpringRunner.class)
@ComponentScan("com.scl.hadoop.hbase")
@SpringBootTest
public class Application {


    @Test
    public void contextLoads() {
    }

}
