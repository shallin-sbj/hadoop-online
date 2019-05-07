package com.scl.template.utils.dao;

import com.scl.template.utils.domain.SmsRecognition;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
@Slf4j
public class SmsRecognitionRepository {

    @Autowired
    private MongoTemplate mongoTemplate;

    public void bathInsert(List<SmsRecognition> layoutFiles) {
        try {
            mongoTemplate.insert(layoutFiles, SmsRecognition.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
