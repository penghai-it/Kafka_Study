package it.ph.com.springboot_kafka_study.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class UserController {
    private static final String PHTEST_TOPIC = "PHTEST_TOPIC";
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/api/v1/{num}")
    public void sendMessage(@PathVariable("num") String num) {

        kafkaTemplate.send(PHTEST_TOPIC, "发送消息num" + num).addCallback(success -> {
            //断言 判断参数条件的
            assert success != null;
            String topic = success.getRecordMetadata().topic();
            int partition = success.getRecordMetadata().partition();
            long offset = success.getRecordMetadata().offset();
            System.out.println("消息发送成功：topic=" + topic + "，partition=" + partition + ",offset=" + offset);
        }, failure -> {
            System.out.println("消息发送失败！失败原因：" + failure.getMessage());
        });


    }

    /**
     * 使用注解的方式进行事物提交
     *
     * @param num
     * @throws Exception
     */
    @GetMapping("/api/v1/tran1")
    @Transactional(rollbackFor = RuntimeException.class)
    public void sendMessage2(int num) {
        kafkaTemplate.send(PHTEST_TOPIC, "这是事务消息1 i=" + num);
        if (num == 0) {
            throw new RuntimeException();
        }
        kafkaTemplate.send(PHTEST_TOPIC, "这是事务消息2 i=" + num);
    }

    /**
     * 申明式事务提交
     *
     * @param num
     */
    @GetMapping("/api/v1/tran2")
    public void sendMessage3(int num) {
        kafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback<String, Object, Object>() {
            @Override
            public Object doInOperations(KafkaOperations<String, Object> kafkaOperations) {
                kafkaOperations.send(PHTEST_TOPIC, "这是事务消息1 i=" + num);
                if (num == 0) {
                    throw new RuntimeException();
                }
                kafkaOperations.send(PHTEST_TOPIC, "这是事务消息2 i=" + num);
                return true;
            }
        });

    }
}