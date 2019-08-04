package com.scl.hadoop.hdfs.mapreduce;

import com.scl.hadoop.hdfs.mapreduce.reducejoin.Emplyee;
import com.scl.hadoop.hdfs.mapreduce.reducejoin.ReduceJoinReducer;
import com.scl.hadoop.hdfs.mapreduce.reducejoin.ReduceJoinReducerMapper;
import com.scl.hadoop.hdfs.mapreduce.secondsort.GroupingComparator;
import com.scl.hadoop.hdfs.mapreduce.secondsort.IntPair;
import com.scl.hadoop.hdfs.mapreduce.secondsort.SecondSortMapper;
import com.scl.hadoop.hdfs.mapreduce.secondsort.SecondSortReducer;
import com.scl.hadoop.hdfs.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class SecordSortAppTest {

    @Autowired
    private HdfsUtils hdfsUtils;

    /**
     * 实现二次排序
     * @throws IOException
     * @throws URISyntaxException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Test
    public void SecordSortAppTest() throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String INPUT_PATH = "/test/secondsort";
        String OUTPUT_PATH = "/test/secondsort/output";

        Configuration conf = hdfsUtils.getConfiguration();
        FileSystem fileSystem = hdfsUtils.getFileSystem();
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }

        Job job = Job.getInstance(conf, "SecondarySortApp");

        // run jar class
        job.setJarByClass(SecordSortAppTest.class);

        // 输入路径
        FileInputFormat.setInputPaths(job, new Path(INPUT_PATH));
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        // 设置Mapper类和设置Reducer
        job.setMapperClass(SecondSortMapper.class);
        job.setReducerClass(SecondSortReducer.class);

        // 分组函数
        job.setGroupingComparatorClass(GroupingComparator.class);

        //设置Map输出类型
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 输入输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
