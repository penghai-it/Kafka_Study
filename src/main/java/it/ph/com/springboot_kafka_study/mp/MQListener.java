package it.ph.com.springboot_kafka_study.mp;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.internals.Topic;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class MQListener {
    /**
     * 消费监听
     *
     * @param record
     */
    @KafkaListener(topics = {"PHTEST_TOPIC"}, groupId = "ph_test_gp1")
    public void onMessage1(ConsumerRecord<?, ?> record, Acknowledgment ack, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        // 打印出消息内容
        System.out.println("消费：" + record.topic() + "-" + record.partition() + "-" + record.value());
        //提交消费偏移量
        ack.acknowledge();
    }


}
