package com.sucl.template.demo.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 统计单词
 */
public class WordCountApp {

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(11);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }


    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
                result.set(sum);
                context.write(key, result);
            }
        }

        public static void main(String[] args) throws Exception {
//            String INPUT_PATH  = "hdfs://su.sulin.online:8020/wc";
//            String OUTPUT_PATH = "hdfs://su.sulin.online:8020/outputwc";
            String INPUT_PATH  = args[0];
            String OUTPUT_PATH = args[1];

            Configuration conf = new Configuration();
//            final FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);
//            if (fileSystem.exists(new Path(OUTPUT_PATH))) {
//                fileSystem.delete(new Path(OUTPUT_PATH), true);
//            }

            Job job = Job.getInstance(conf, "WordCountApp");
            //  运行jar 类
            job.setJarByClass(WordCountApp.class);
            // 设置 map
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // 设置 reduce
            job.setReducerClass(MyReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            // 设置输入格式
            job.setInputFormatClass(TextInputFormat.class);
            Path inputPath = new Path(INPUT_PATH);
            FileInputFormat.addInputPath(job, inputPath);

            // 设置输出格式
            job.setOutputFormatClass(TextOutputFormat.class);
            Path outputPath = new Path(OUTPUT_PATH);
            FileOutputFormat.setOutputPath(job, outputPath);
            // 提交job
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
