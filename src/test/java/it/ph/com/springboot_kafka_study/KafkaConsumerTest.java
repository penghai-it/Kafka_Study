package it.ph.com.springboot_kafka_study;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者配置
 */
public class KafkaConsumerTest {

    public static Properties getProperties() {
        Properties props = new Properties();
        //broker地址
        props.put("bootstrap.servers", "123.57.214.53:9092");
        //消费者分组ID，分组内的消费者只能消费该消息一次，不同分组内的消费者可以重复消费该消息
        props.put("group.id", "xdclass-g11");
        //默认是latest，如果需要从头消费partition消息，需要改为 earliest 且消费者组名变更(及消费者分组的id)，才生效
        props.put("auto.offset.reset", "earliest");
        //开启自动提交offset
        //props.put("enable.auto.commit", "true");
        //自动提交offset延迟时间
        // props.put("auto.commit.interval.ms", "1000");
        //手动提交(这种就不需要配置延时时间)
        props.put("enable.auto.commit", "false");
        //反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    @Test
    public void simpleConsumerTest() {
        Properties props = getProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅topic主题
        consumer.subscribe(List.of(KafkaAdminTest.TOPIC_NAME));
        //因为是当前线程走完就会停止，所以写一个死循环（正式是开发是不建议的）
        while (true) {
            //拉取时间控制，阻塞超时时间
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                System.err.printf("topic = %s, offset = %d, key = %s, value = %s%n", record.topic(), record.offset(), record.key(), record.value());
            }
            //同步阻塞提交（生产环境中一般不这样使用）
            //consumer.commitSync();

            //消息不为空的时候才进行提交（下面的代码只有手动提交时才需要，自动提交时不需要）
            if (!records.isEmpty()) {
                //异步提交
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception == null) {
                            System.out.println("手动提交成功！" + String.valueOf(offsets));
                        } else {
                            System.out.println("手动提交失败！" + String.valueOf(offsets));
                            exception.printStackTrace();
                        }
                    }
                });
            }
        }
    }
}
