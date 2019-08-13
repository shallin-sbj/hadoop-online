package com.scl.online.hadoop.hdfs.mapreduce.work;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TrafficMapper extends Mapper<LongWritable, Text, Text, TrafficWritable> {

    private Text outputKey = new Text();
    private TrafficWritable outputValue = new TrafficWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String lineValue = value.toString();

        String[] strs = lineValue.split("\t");

        String phoneNum = strs[1];
        int upPackNum = Integer.valueOf(strs[6]);
        int downPackNum = Integer.valueOf(strs[7]);
        int upPayLoad = Integer.valueOf(strs[8]);
        int downPayLoad = Integer.valueOf(strs[9]);

        // set
        outputKey.set(phoneNum);
        outputValue.set(upPackNum, downPackNum, upPayLoad, downPayLoad);

        // context write
        context.write(outputKey, outputValue);
    }
}
