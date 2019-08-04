package com.scl.hadoop.hdfs.utils;

import com.scl.hadoop.hdfs.config.HdfsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class HadoopBasicUtils {

    @Autowired
    private HdfsConfig hdfsConfig;

    /**
     * 获取HDFS文件系统
     *
     * @return org.apache.hadoop.fs.FileSystem
     */
    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(getConfiguration());
    }

    public Configuration getConfiguration() {
        return hdfsConfig.configuration();
    }


}
