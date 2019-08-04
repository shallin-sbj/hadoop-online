package com.scl.hadoop.hdfs.mapreduce;

import com.scl.hadoop.hdfs.mapreduce.wordcount.MyMapper;
import com.scl.hadoop.hdfs.mapreduce.wordcount.MyReducer;
import com.scl.hadoop.hdfs.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.URISyntaxException;


@RunWith(SpringRunner.class)
@ComponentScan("com.scl.hadoop.hdfs")
@SpringBootTest
public class WordCountAppTest {

    @Autowired
    private HdfsUtils hdfsUtils;


    @Test
    public void WordCountAppTest() throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String INPUT_PATH = "/wc";
        String OUTPUT_PATH = "/wc/output";

        Configuration conf = hdfsUtils.getConfiguration();
        FileSystem fileSystem = hdfsUtils.getFileSystem();
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }

        Job job = Job.getInstance(conf, "WordCountAppTest");

        // run jar class
        job.setJarByClass(WordCountAppTest.class);

        // 设置map
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置reduce
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置input formart
        job.setInputFormatClass(TextInputFormat.class);
        Path inputPath = new Path(INPUT_PATH);
        FileInputFormat.addInputPath(job, inputPath);

        // 设置output format
        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputPath = new Path(OUTPUT_PATH);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

}
