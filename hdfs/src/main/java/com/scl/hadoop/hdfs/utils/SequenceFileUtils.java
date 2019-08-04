package com.scl.hadoop.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Service
public class SequenceFileUtils {


    @Autowired
    private HadoopBasicUtils basicUtils;


    /**
     * SequenceFile写操作
     * @param pathStri
     * @throws IOException
     */
    public void sequenceFileWriter(String pathStri,String[] data) throws IOException {
        FileSystem fileSystem = basicUtils.getFileSystem();
        Configuration configuration = basicUtils.getConfiguration();

        Path outputPath = new Path(pathStri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem, configuration, outputPath, IntWritable.class, Text.class);
        for (int i = 0; i < 10; i++) {
            key.set(10 - i);
            value.set(data[i % data.length]);
            writer.append(key, value);
        }
        IOUtils.closeStream(writer);
    }


    public Map sequenceFileRead(String pathStr) throws IOException {
        FileSystem fileSystem = basicUtils.getFileSystem();
        Configuration configuration = basicUtils.getConfiguration();
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, new Path(pathStr), configuration);

        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
        Map<Object,Object> map = new HashMap<>();
        while (reader.next(key, value)) {
//            System.out.printf("%s\t%s\n", key, value);

            while(reader.next(key, value)){
                System.out.println("key : " + key);
                System.out.println("value : " + value);
                System.out.println("position : " + reader.getPosition());
            }

            map.put(key,value);
        }
        IOUtils.closeStream(reader);
        return map;
    }

    public void sequenceFileCompression(String inputPathStr,String outputPathStr,String[] data) throws IOException {
        FileSystem fileSystem = basicUtils.getFileSystem();
        Configuration configuration = basicUtils.getConfiguration();

        Path outputPath = new Path(outputPathStr);

        IntWritable key = new IntWritable();
        Text value = new Text();

        SequenceFile.Writer writer = SequenceFile.createWriter(fileSystem,
                configuration, outputPath, IntWritable.class, Text.class,
                SequenceFile.CompressionType.RECORD, new BZip2Codec());

        for (int i = 0; i < 10; i++) {
            key.set(10 - i);
            value.set(data[i % data.length]);
            writer.append(key, value);
        }
        IOUtils.closeStream(writer);

        Path inputPath = new Path(inputPathStr);
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem,inputPath,configuration);

        Writable keyClass = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
        Writable valueClass = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
        while(reader.next(keyClass, valueClass)){
            System.out.println("key : " + keyClass);
            System.out.println("value : " + valueClass);
            System.out.println("position : " + reader.getPosition());
        }
        IOUtils.closeStream(reader);

    }


}