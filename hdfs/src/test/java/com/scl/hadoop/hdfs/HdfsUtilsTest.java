package com.scl.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class HdfsUtilsTest {

    @Test
    public void testRead() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path path = new Path("hdfs://s201/usr/centos/hell.txt");
        String string = HdfsUtils.readToString(configuration, path);
        System.out.printf(string);
    }

    @Test
    public void testWrite() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path path = new Path("hdfs://s201/usr/centos/helleworld.txt");
        HdfsUtils.createPath(configuration, path);
    }
}
