package com.scl.hadoop.hdfs.mapreduce;

import com.scl.hadoop.hdfs.mapreduce.reducejoin.Emplyee;
import com.scl.hadoop.hdfs.mapreduce.reducejoin.ReduceJoinReducer;
import com.scl.hadoop.hdfs.mapreduce.reducejoin.ReduceJoinReducerMapper;
import com.scl.hadoop.hdfs.mapreduce.sort.SortMapper;
import com.scl.hadoop.hdfs.mapreduce.sort.SortReducer;
import com.scl.hadoop.hdfs.mapreduce.wordcount.MyMapper;
import com.scl.hadoop.hdfs.mapreduce.wordcount.MyReducer;
import com.scl.hadoop.hdfs.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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
public class SortAppTest {

    @Autowired
    private HdfsUtils hdfsUtils;

    /**
     * 使用MapReduce API实现排序
     * @throws IOException
     * @throws URISyntaxException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Test
    public void SortAppTest() throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String INPUT_PATH = "/test/inputjoin";
        String OUTPUT_PATH = "/test/outputmapjoin";

        Configuration conf = hdfsUtils.getConfiguration();
        FileSystem fileSystem = hdfsUtils.getFileSystem();
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }

        Job job = Job.getInstance(conf, "SortApp");

        // run jar class
        job.setJarByClass(SortAppTest.class);

        //设置Map和Reduce处理类
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        //设置输出类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
