package com.scl.template.utils.domain;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "SmsRecognition")
public class SmsRecognition {

    @Id
    private ObjectId id;

    private String brand;

    private String pub_id;

    private String province;

    private String city;

    private String ac;

    private String rc;

    private String ac_act_imei;

    private String rc_act_imei;

    private String dt;

}
