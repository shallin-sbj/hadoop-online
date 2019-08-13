package com.scl.online.hadoop.hdfs;

import com.scl.online.hadoop.hdfs.domain.User;
import com.scl.online.hadoop.hdfs.utils.HdfsUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

/**
 * 测试HDFS的基本操作
 */
@RunWith(SpringRunner.class)
@ComponentScan("com.scl.hadoop.hdfs")
@SpringBootTest
public class HdfsApplication {

    @Autowired
    private HdfsUtils hdfsUtils;

    @Test
    public void contextLoads() {
    }


    /**
     * 测试创建HDFS目录
     */
    @Test
    public void testMkdir() {
        boolean result1 = hdfsUtils.mkdir("/testDir");
        System.out.println("创建结果：" + result1);
        boolean result2 = hdfsUtils.mkdir("/testDir/subDir");
        System.out.println("创建结果：" + result2);

        testUploadFile();
    }

    /**
     * 测试上传文件
     */
    @Test
    public void testUploadFile() {
        //TODO
        //测试上传三个文件
        hdfsUtils.uploadFileToHdfs("C:/Users/User/Desktop/a.txt", "/testDir");
        hdfsUtils.uploadFileToHdfs("C:/Users/User/Desktop/b.txt", "/testDir");
        hdfsUtils.uploadFileToHdfs("C:/Users/User/Desktop/c.txt", "/testDir/subDir");

//        hdfsService.uploadFileToHdfs("C:/Users/User/Desktop/user.txt", "/testDir");
    }

    /**
     * 测试列出某个目录下面的文件
     */
    @Test
    public void testListFiles() {
        List<Map<String, Object>> result = hdfsUtils.listFiles("/testDir", null);

        result.forEach(fileMap -> {
            fileMap.forEach((key, value) -> {
                System.out.println(key + "--" + value);
            });
            System.out.println();
        });
    }

    /**
     * 测试下载文件
     */
    @Test
    public void testDownloadFile() {
        //TODO
        hdfsUtils.downloadFileFromHdfs("/testDir/a.txt", "C:/Users/User/Desktop/test12.txt");
    }

    /**
     * 测试打开HDFS上面的文件
     */
    @Test
    public void testOpen() throws IOException {

        FSDataInputStream inputStream = hdfsUtils.open("/testDir/a.txt");

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        reader.close();
    }
    /**
     * 测试打开HDFS上面的文件，并转化为Java对象
     */
    @Test
    public void testOpenWithObject() throws IOException {
        User user = hdfsUtils.openWithObject("/testDir/user.txt", User.class);
        System.out.println(user.toString());
    }

    /**
     * 测试重命名
     */
    @Test
    public void testRename(){
        hdfsUtils.rename("/testDir/b.txt","/testDir/b_new.txt");
        //再次遍历
        testListFiles();
    }


    /**
     * 测试删除文件
     */
    @Test
    public void testDelete(){
        hdfsUtils.delete("/testDir/b_new.txt");

        //再次遍历
        testListFiles();
    }

    /**
     * 测试合并文件
     */
    @Test
    public void testGetFileBlockLocations() throws IOException {
        String srcPath = "/testDir/";
        String destPath ="/testDir/subDir/";
        hdfsUtils.merge(srcPath,destPath);

////        FSDataInputStream inputStream = hdfsUtils.open(destPath);
//        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//        String line = null;
//        while ((line = reader.readLine()) != null) {
//            System.out.println(line);
//        }
//        reader.close();

    }

}
