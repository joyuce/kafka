package com.j.openproject.controller;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;

import com.j.openproject.annotation.RestPathController;

/**
 * @author Joyuce
 * @Type KafkaController
 * @Desc
 * @date 2020年02月26日
 * @Version V1.0
 */
@RestPathController("kafka")
public class KafkaController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private NewTopic newTopic;

    @RequestMapping("send")
    public String send(String msg) {
        for (int i = 0; i < 12; i++) {
            kafkaTemplate.send(newTopic.name(), "test batch listener,dataNum-" + i);
        }
        return "success";
    }
}
