package com.scl.online.hadoop.hdfs.mapreduce.work;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrafficReducer extends Reducer<Text, TrafficWritable, Text, TrafficWritable> {

    private TrafficWritable outputValue = new TrafficWritable();

    @Override
    protected void reduce(Text key, Iterable<TrafficWritable> v2s,
                          Context context) throws IOException, InterruptedException {
        int upPackNum = 0;
        int downPackNum = 0;
        int upPayLoad = 0;
        int downPayLoad = 0;

        for (TrafficWritable value : v2s) {
            upPackNum += value.getUpPackNum();
            downPackNum += value.getDownPackNum();
            upPayLoad += value.getUpPayLoad();
            downPayLoad += value.getDownPayLoad();
        }

        // set
        outputValue.set(upPackNum, downPackNum, upPayLoad, downPayLoad);

        // context write
        context.write(key, outputValue);
    }
}
