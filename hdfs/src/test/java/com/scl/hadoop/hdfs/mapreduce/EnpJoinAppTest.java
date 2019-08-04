package com.scl.hadoop.hdfs.mapreduce;

import com.scl.hadoop.hdfs.mapreduce.recordreader.RecordReaderInputFormat;
import com.scl.hadoop.hdfs.mapreduce.recordreader.RecordReaderMapper;
import com.scl.hadoop.hdfs.mapreduce.recordreader.RecordReaderPartitioner;
import com.scl.hadoop.hdfs.mapreduce.recordreader.RecordReaderReducer;
import com.scl.hadoop.hdfs.mapreduce.reducejoin.Emplyee;
import com.scl.hadoop.hdfs.mapreduce.reducejoin.ReduceJoinReducer;
import com.scl.hadoop.hdfs.mapreduce.reducejoin.ReduceJoinReducerMapper;
import com.scl.hadoop.hdfs.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
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
public class EnpJoinAppTest {

    @Autowired
    private HdfsUtils hdfsUtils;

    @Test
    public void EnpJoinAppTest() throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String INPUT_PATH = "/test/inputjoin";
        String OUTPUT_PATH = "/test/outputmapjoin";

        Configuration conf = hdfsUtils.getConfiguration();
        FileSystem fileSystem = hdfsUtils.getFileSystem();
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }

        Job job = Job.getInstance(conf, "Reduce Join");

        // run jar class
        job.setJarByClass(EnpJoinAppTest.class);


        // 设置Mapper类和设置Reducer
        job.setMapperClass(ReduceJoinReducerMapper.class);
        job.setReducerClass(ReduceJoinReducer.class);

        //设置Map输出类型
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Emplyee.class);

        //设置Reduce输出类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Emplyee.class);

        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
