package com.scl.online.hadoop.hdfs.mapreduce.recordreader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RecordReaderReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

    // 创建写出去的key和value
    private Text outKey = new Text();
    private LongWritable outValue = new LongWritable();

    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {

        System.out.println("奇数行还是偶数行：" + key);

        // 定义求和的变量
        long sum = 0;
        // 遍历value求和
        for (Text val : values) {
            // 累加
            sum += Long.parseLong(val.toString());
        }

        // 判断奇偶数
        if (key.get() == 0) {
            outKey.set("奇数之和为：");
        } else {
            outKey.set("偶数之和为：");

        }
        // 设置value
        outValue.set(sum);

        // 把结果写出去
        context.write(outKey, outValue);
    }
}
