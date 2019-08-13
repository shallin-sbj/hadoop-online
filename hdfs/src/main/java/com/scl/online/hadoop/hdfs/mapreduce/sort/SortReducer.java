package com.scl.online.hadoop.hdfs.mapreduce.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends
        Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private static IntWritable data = new IntWritable(1);

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context)
            throws IOException, InterruptedException {
        for (IntWritable val : values) {
            context.write(data, key);
            data = new IntWritable(data.get() + 1);
        }
    }
}
