package com.scl.hadoop.hdfs.mapreduce.recordreader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecordReaderMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException,
            InterruptedException {
        // 直接将读取的记录写出去
        context.write(key, value);
    }
}
