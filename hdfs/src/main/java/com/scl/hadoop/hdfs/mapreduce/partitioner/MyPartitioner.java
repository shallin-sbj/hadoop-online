package com.scl.hadoop.hdfs.mapreduce.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {


    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        if (text.toString().equals("xiaomi"))
            return 0;
        if (text.toString().equals("huawei"))
            return 1;
        if (text.toString().equals("iphone7"))
            return 2;
        return 3;
    }
}
