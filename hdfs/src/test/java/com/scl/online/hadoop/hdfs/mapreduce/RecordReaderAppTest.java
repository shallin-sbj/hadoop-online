package com.scl.online.hadoop.hdfs.mapreduce;

import com.scl.online.hadoop.hdfs.mapreduce.recordreader.RecordReaderInputFormat;
import com.scl.online.hadoop.hdfs.mapreduce.recordreader.RecordReaderMapper;
import com.scl.online.hadoop.hdfs.mapreduce.recordreader.RecordReaderPartitioner;
import com.scl.online.hadoop.hdfs.mapreduce.recordreader.RecordReaderReducer;
import com.scl.online.hadoop.hdfs.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
public class RecordReaderAppTest {

    @Autowired
    private HdfsUtils hdfsUtils;

    /**
     * RedcordReader 操作
     * @throws IOException
     * @throws URISyntaxException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Test
    public void RecordReaderAppTest() throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String INPUT_PATH = "/test/bigdata.txt";
        String OUTPUT_PATH = "/test/outputrecordreader";

//        String INPUT_PATH = "/wc";
//        String OUTPUT_PATH = "/wc/output";

        Configuration conf = hdfsUtils.getConfiguration();
        FileSystem fileSystem = hdfsUtils.getFileSystem();
        if (fileSystem.exists(new Path(OUTPUT_PATH))) {
            fileSystem.delete(new Path(OUTPUT_PATH), true);
        }

        Job job = Job.getInstance(conf, "RecordReaderApp");

        // run jar class
        job.setJarByClass(RecordReaderAppTest.class);

        // 1.1 设置输入目录和设置输入数据格式化的类
        FileInputFormat.setInputPaths(job, INPUT_PATH);
        job.setInputFormatClass(RecordReaderInputFormat.class);

        // 1.2 设置自定义Mapper类和设置map函数输出数据的key和value的类型
        job.setMapperClass(RecordReaderMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 1.3 设置分区和reduce数量(reduce的数量，和分区的数量对应，因为分区为一个，所以reduce的数量也是一个)
        job.setPartitionerClass(RecordReaderPartitioner.class);
        job.setNumReduceTasks(2);

        // 2.1 Shuffle把数据从Map端拷贝到Reduce端。
        // 2.2 指定Reducer类和输出key和value的类型
        job.setReducerClass(RecordReaderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 2.3 指定输出的路径和设置输出的格式化类
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.setOutputFormatClass(TextOutputFormat.class);
        // 提交job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
