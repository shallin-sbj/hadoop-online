package com.scl.hadoop.hdfs.mapreduce.secondsort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SecondSortMapper extends Mapper<LongWritable, Text, IntPair, IntWritable> {

    private final IntPair key = new IntPair();
    private final IntWritable value = new IntWritable();

    @Override
    public void map(LongWritable inKey, Text inValue,
                    Mapper.Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(inValue.toString());
        int left = 0;
        int right = 0;
        if (itr.hasMoreTokens()) {
            left = Integer.parseInt(itr.nextToken());
            if (itr.hasMoreTokens()) {
                right = Integer.parseInt(itr.nextToken());
            }
            key.set(left, right);
            value.set(right);
            context.write(key, value);
        }
    }
}
