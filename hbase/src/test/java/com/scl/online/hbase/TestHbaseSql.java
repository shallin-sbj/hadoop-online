package com.scl.online.hbase;

import com.scl.online.hbase.repository.HBaseService;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 测试Hbase SQL
 */
@RunWith(SpringJUnit4ClassRunner.class)
@Slf4j
@SpringBootTest
@WebAppConfiguration
public class TestHbaseSql {

    @Autowired
    private HBaseService hbaseService;

    /**
     * 测试删除、创建表
     */
    @Test
    public void testCreateTable() throws IOException {
        //删除表
        hbaseService.deleteTable("test_base");

        //创建表
        hbaseService.createTableBySplitKeys("test_base", Arrays.asList("f", "back"), hbaseService.getSplitKeys(null));

        // 插入三条数据
        hbaseService.putData("test_base", "66804_000001", "f", new String[]{"project_id", "varName", "coefs", "pvalues", "tvalues", "create_time"}, new String[]{"40866", "mob_3", "0.9416", "0.0000", "12.2293", "null"});
        hbaseService.putData("test_base", "66804_000002", "f", new String[]{"project_id", "varName", "coefs", "pvalues", "tvalues", "create_time"}, new String[]{"40866", "idno_prov", "0.9317", "0.0000", "9.8679", "null"});
        hbaseService.putData("test_base", "66804_000003", "f", new String[]{"project_id", "varName", "coefs", "pvalues", "tvalues", "create_time"}, new String[]{"40866", "education", "0.8984", "0.0000", "25.5649", "null"});

        //查询数据
        //1. 根据rowKey查询
        Map<String, String> result1 = hbaseService.getRowData("test_base", "66804_000001");
        System.out.println("+++++++++++根据rowKey查询+++++++++++");
        result1.forEach((k, value) -> {
            System.out.println(k + "---" + value);
        });
        System.out.println();
//
//        //精确查询某个单元格的数据
        String str1 = hbaseService.getColumnValue("test_base", "66804_000002", "f", "area_button");
        System.out.println("+++++++++++精确查询某个单元格的数据+++++++++++");
        System.out.println(str1);
        // 需要时写到文件中
//        writeFile("test_base", result2);

//        // 模糊查询数据
//        String tableName = "data:area_button";
//        String prefix = "CN:6800184-20190701";
//        long startPage= System.currentTimeMillis();
//        Map<String, Map<String, String>> result = hbaseService.getResultScannerPrefixFilter(tableName, prefix);
//        result.forEach((k, value) -> {
//            log.info(k + "---" + value);
//        });
//        long spentTimes= System.currentTimeMillis() - startPage;
//        log.info("page query: SPEND TIME:{} ", spentTimes + " data size :" + result.size());
//        log.info("+++++++++++遍历查询+++++++++++");
//
//        writeFile(tableName,result);

    }


    private void writeFile(String fileName, Map<String, Map<String, String>> result2) {
        if (!result2.isEmpty()) {
            FileWriter fileWriter = null;
            try {
                fileName = fileName.substring(fileName.lastIndexOf(":") + 1);
                fileWriter = new FileWriter("C:\\Users\\User\\Desktop\\" + fileName + ".txt");//创建文本文件
                log.info("fileName:" + fileName);
                AtomicInteger i = new AtomicInteger();
                FileWriter finalFileWriter = fileWriter;
                result2.forEach((k, value) -> {
                    String info = k + "---" + value;
                    try {
                        finalFileWriter.write(info + "\r\n");//写入 \r\n换行
                        i.getAndIncrement();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                fileWriter.write("共" + i + "条");
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
