package com.scl.hadoop.hdfs;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

public class HdfsUtils {

    /**
     * 读取文件
     *
     * @param configuration
     * @param path
     * @return
     * @throws IOException
     */
    public static String readToString(Configuration configuration, Path path) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream fis = fs.open(path);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copyBytes(fis, baos, 1024);
        fis.close();
        return new String(baos.toByteArray());
    }

    /**
     * 创建文件
     *
     * @param configuration
     * @param path
     * @throws IOException
     */

    public static void createPath(Configuration configuration, Path path) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FSDataOutputStream fsDataOutputStream = fs.create(path);
        fsDataOutputStream.close();
    }

}
