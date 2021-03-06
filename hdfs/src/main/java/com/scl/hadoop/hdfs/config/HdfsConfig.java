package com.scl.hadoop.hdfs.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * HDFS相关配置
 */
@Configuration
public class HdfsConfig {

    @Value("${hdfs.defaultFS}")
    private String defaultHdfsUri;

    @Bean
    public void configuration() {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", defaultHdfsUri);
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
    }

}
