package com.j.openproject.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Joyuce
 * @Type TestListener
 * @Desc
 * @date 2020年02月26日
 * @Version V1.0
 */
@Slf4j
@Component
public class TestListener {

    @Autowired
    private KafkaProperties kafkaProperties;


    @Bean("batchContainerFactory")
    @SuppressWarnings("unchecked")
    public ConcurrentKafkaListenerContainerFactory listenerContainer() {
        ConcurrentKafkaListenerContainerFactory container = new ConcurrentKafkaListenerContainerFactory();
        container.setConsumerFactory(new DefaultKafkaConsumerFactory(kafkaProperties.buildConsumerProperties()));
        //设置并发量，小于或等于Topic的分区数
        container.setConcurrency(5);
        //设置为是否批量接收消息
        container.setBatchListener(false);
        return container;
    }

    @Bean
    public NewTopic batchTopic() {
        return new NewTopic("topic.quick.batch", 8, (short) 1);
    }

    @KafkaListener(id = "batch", clientIdPrefix = "batch", topics = {
            "topic.quick.batch" }, containerFactory = "batchContainerFactory")
    public void listener(String data) {
        log.info("topic.quick.batch  receive : " + data);
    }

}
