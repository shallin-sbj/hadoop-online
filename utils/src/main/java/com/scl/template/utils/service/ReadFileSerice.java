package com.scl.template.utils.service;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.scl.template.utils.config.MongoDBConifg;
import com.scl.template.utils.dao.SmsRecognitionRepository;
import com.scl.template.utils.domain.SmsRecognition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class ReadFileSerice {

    @Autowired
    private MongoDBConifg mongoDBConifg;

    @Value("${spring.sucl.basepath}")
    private String basepath;

    @Value("${spring.sucl.filename}")
    private String filename;

    @Value("${spring.sucl.pagesize}")
    private int pagesize;

//    @Value("${spring.sucl.collectionName}")
//    private int collectionName;

    @Autowired
    private SmsRecognitionRepository smsRecognitionRepository;

    @PostConstruct
    private boolean syncFile() throws IOException {
        log.info("start ............. you are sb   ");
        File file = new File(basepath, filename);
        if (file.isFile() && file.canRead()) {
            FileInputStream fis = null;
            InputStreamReader isr = null;
            BufferedReader br = null;
            try {
                fis = new FileInputStream(file);
                isr = new InputStreamReader(fis, StandardCharsets.UTF_8);
                br = new BufferedReader(isr, 1024 * 1024 * 16);
                List<String> list = new ArrayList<>(pagesize);

                int i = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    if ((line = line.trim()).isEmpty()) {
                        continue;
                    }

                    list.add(line);
                    if (++i % pagesize == 0) {
                        boolean success = saveToDB(filename, list);
                        log.info("FILENAME:{} LIST_SIZE:{}  SUCCESS:{}", filename, i, success);

                        if (!success) {
                            return false;
                        }

                        list = new ArrayList<>(pagesize);
                    }
                }

                boolean success = saveToDB(filename, list);
                log.info("FILENAME:{} LIST_SIZE:{}  SUCCESS:{}", filename, i, success);
                if (!success) {
                    return false;
                }
            } catch (IOException e) {
                log.error("READ_FILE_MEET_EXCEPTION:" + e.getMessage(), e);
                return false;
            } finally {
                br.close();
                isr.close();
                fis.close();
            }
            return true;
        }
        log.info("end  ............. you are sb   ");
        return false;
    }

    private boolean saveToDB(String filename, List<String> subList) {
        log.info("saveToDB .............    ");
        if (CollectionUtils.isEmpty(subList)) {
            return true;
        }
        try {
            Gson gson = new Gson();
            List<SmsRecognition> batchSaveList = Lists.newArrayListWithCapacity(subList.size());
            for (String line : subList) {
                try {
                    SmsRecognition info = gson.fromJson(line, SmsRecognition.class);
                    batchSaveList.add(info);
                } catch (Exception e) {
                    log.error("CONVERT_MEET_EXCEPTION,FILENAME:{}  {}", filename, e.getMessage(), e);
                    log.error("CONVERT_MEET_EXCEPTION,FILENAME:{}  {}", filename, line);
                }

            }
            MongoClient client = mongoDBConifg.getClient();
            ClientSession session = client.startSession();
            MongoCollection<SmsRecognition> smsRecognition = mongoDBConifg.getMongoDatabase().getCollection("smsRecognition", SmsRecognition.class);
            smsRecognitionRepository.bathInsert(session, smsRecognition, batchSaveList, false);
            return true;
        } catch (Throwable e) {
            log.info(e.getMessage(), e);
            return false;
        }
    }


}
