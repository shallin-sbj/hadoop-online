package com.scl.hadoop.hdfs.utils;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Service;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

@Slf4j
@RunWith(SpringRunner.class)
@ComponentScan("com.scl.hadoop.hdfs")
@SpringBootTest
public class SequenceFileUtilsTest {

    @Autowired
    private SequenceFileUtils sequenceFileUtils;

    @Test
    public void contextLoads() {
    }

    @Test
    public void sequenceFileWriter() throws IOException {
        String[] data = {"a,b,c,d,e,f,g", "e,f,g,h,i,j,k", "l,m,n,o,p,q,r,s", "t,u,v,w,x,y,z"};
        String pathStr = "/test/subDir";
        sequenceFileUtils.sequenceFileWriter(pathStr, data);

    }

    @Test
    public void sequenceFileReader() throws IOException {
        String[] data = {"a,b,c,d,e,f,g", "e,f,g,h,i,j,k", "l,m,n,o,p,q,r,s", "t,u,v,w,x,y,z"};
        String pathStr = "/test/subDir";
        Map map = sequenceFileUtils.sequenceFileRead(pathStr);
        Set set = map.entrySet();
        Iterator iterator = set.iterator();
        if (iterator.hasNext()) {
            Object next = iterator.next();
            System.out.println(next);
        }
    }

    @Test
    public void sequenceFileCompression() throws IOException {
        String[] data = {"a,b,c,d,e,f,g", "e,f,g,h,i,j,k", "l,m,n,o,p,q,r,s", "t,u,v,w,x,y,z"};
        String inputPathStr = "/test/subDir";
        String outputPathStr = "/test/subDirout/";
        sequenceFileUtils.sequenceFileCompression(inputPathStr, outputPathStr, data);

    }

}
