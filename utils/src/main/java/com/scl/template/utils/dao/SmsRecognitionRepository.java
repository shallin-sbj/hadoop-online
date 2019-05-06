package com.scl.template.utils.dao;

import com.google.gson.Gson;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.scl.template.utils.domain.SmsRecognition;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
@Slf4j
public class SmsRecognitionRepository {

    /**
     * @param session
     * @param collection
     * @param layoutFiles
     * @param ordered     如果为真，那么当写失败时，返回时不执行剩余的写操作。如果为false，那么当写入失败时，继续执行剩余的写入操作(如果有的话)。默认值为true。
     * @return
     */
    public BulkWriteResult bathInsert(ClientSession session, MongoCollection<SmsRecognition> collection, List<SmsRecognition> layoutFiles, boolean ordered) {
        BulkWriteResult bulkWriteResult = null;
        Gson gson = new Gson();
        List<InsertOneModel<SmsRecognition>> insertOneModels = new ArrayList<>();
        for (SmsRecognition layoutFile : layoutFiles) {
            if (log.isDebugEnabled()) {
                log.info("bath insert:" + layoutFile.toString());
            }
            Document document = Document.parse(gson.toJson(layoutFile));
            InsertOneModel<SmsRecognition> model = new InsertOneModel(document);
            insertOneModels.add(model);
        }
        try {
            bulkWriteResult = collection.bulkWrite(session, insertOneModels, new BulkWriteOptions().ordered(ordered));
        } catch (Exception e) {
            log.info("insetLayoutFileList  error:", e);
            throw e;
        }
        return bulkWriteResult;
    }

}
