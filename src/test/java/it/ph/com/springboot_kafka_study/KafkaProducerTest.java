package it.ph.com.springboot_kafka_study;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

import javax.swing.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerTest {

    public static Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "123.57.214.53:9092,123.57.214.53:9093");
        //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "112.74.55.160:9092");
        // 当producer向leader发送数据时，可以通过request.required.acks参数来设置数据可靠性的级别,分别是0, 1，all。
        props.put("acks", "all");
        //props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 请求失败，生产者会自动重试，指定是0次，如果启用重试，则会有重复消息的可能性
        props.put("retries", 0);
        //props.put(ProducerConfig.RETRIES_CONFIG, 0);
        // 生产者缓存每个分区未发送的消息,缓存的大小是通过 batch.size 配置指定的，默认值是16KB
        props.put("batch.size", 16384);
        /**
         * 默认值就是0，消息是立刻发送的，即便batch.size缓冲空间还没有满
         * 如果想减少请求的数量，可以设置 linger.ms 大于0，即消息在缓冲区保留的时间，超过设置的值就会被提交到          服务端
         * 通俗解释是，本该早就发出去的消息被迫至少等待了linger.ms时间，相对于这时间内积累了更多消息，批量发送           减少请求
         * 如果batch被填满或者linger.ms达到上限，满足其中一个就会被发送
         */
        props.put("linger.ms", 1);
        /**
         * buffer.memory的用来约束Kafka Producer能够使用的内存缓冲的大小的，默认值32MB。
         * 如果buffer.memory设置的太小，可能导致消息快速的写入内存缓冲里，但Sender线程来不及把消息发送到             Kafka服务器
         * 会造成内存缓冲很快就被写满，而一旦被写满，就会阻塞用户线程，不让继续往Kafka写消息了
         * buffer.memory要大于batch.size，否则会报申请内存不#足的错误，不要超过物理内存，根据实际情况调整
         * 需要结合实际业务情况压测进行配置
         */
        props.put("buffer.memory", 33554432);
        /**
         * key的序列化器，将用户提供的 key和value对象ProducerRecord 进行序列化处理，key.serializer必须被          设置，
         * 即使消息中没有指定key，序列化器必须是一个实
         org.apache.kafka.common.serialization.Serializer接口的类，
         * 将key序列化成字节数组。
         */
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    /**
     * 发送消息
     * <p>
     * send()方法是异步的，添加消息到缓冲区等待发送，并立即返回
     * 生产者将单个的消息批量在一起发送来提高效率,即 batch.size和linger.ms结合
     * <p>
     * 实现同步发送：一条消息发送之后，会阻塞当前线程，直至返回 ack
     * 发送消息后返回的一个 Future 对象，调用get即可
     * <p>
     * 消息发送主要是两个线程：一个是Main用户主线程，一个是Sender线程
     * 1)main线程发送消息到RecordAccumulator即返回
     * 2)sender线程从RecordAccumulator拉取信息发送到broker
     * 3) batch.size和linger.ms两个参数可以影响 sender 线程发送次数
     */
    @Test
    public void testSend() {
        Properties properties = getProperties();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        int i = 0;
        while (i < 6) {
            /**
             * TOPIC_NAME  topic名
             * key         分区的key
             * value       发送的消息
             */
            Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(KafkaAdminTest.TOPIC_NAME, "指定分区的key" + i, "发送的消息" + i));
            try {
                //这一步是用来看发送后返回的数据的，不是必须要的
                RecordMetadata recordMetadata = send.get();
                System.err.println("发送状态：" + String.valueOf(recordMetadata));
                /**
                 * 返回结果 JAVA_TOPIC-4@0
                 * JAVA_TOPIC    topic名
                 * 4             Partitions分区编号
                 * @             链接符没意义
                 * 0             消息的偏移量
                 */
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            i++;
        }
        //使用完需要关闭
        kafkaProducer.close();
    }

    /**
     * 发送消息携带回调函数
     */
    @Test
    public void testSendWithCallback() {
        Properties properties = getProperties();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 3; i++) {
            kafkaProducer.send(new ProducerRecord<>(KafkaAdminTest.TOPIC_NAME, "指定分区的key" + i, "发送的消息" + i), new Callback() {
                // Callback的回调函数
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    //如果exception等于空则表示消息发送成功
                    if (exception == null) {
                        System.out.println(String.valueOf("消息发送状态：" + recordMetadata));
                    } else {
                        System.out.println(String.valueOf("消息发送失败" + exception));
                        exception.printStackTrace();
                    }
                }
            });
        }
        kafkaProducer.close();
    }

    /**
     * 发送消息到指定Producer分区中（指定分区ID即可）
     */
    @Test
    public void testSendWithCallbackById() {
        Properties properties = getProperties();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>(KafkaAdminTest.TOPIC_NAME, i, "指定分区的k" + "ey" + i, "发送的消息" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("消息发送成功：" + String.valueOf(recordMetadata));
                    } else {
                        System.out.println("消息发送失败！" + String.valueOf(exception));
                        exception.printStackTrace();
                    }
                }
            });
        }
        kafkaProducer.close();
    }

    /**
     * 自定义分区策略 发送消息
     */
    @Test
    public void testSendWithPartitonStrategy() {
        Properties properties = getProperties();
        properties.put("partitioner.class", "it.ph.com.springboot_kafka_study.config.PhPartitioner");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>(KafkaAdminTest.TOPIC_NAME, "testKey", "发送的消息" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("消息发送成功！" + String.valueOf(recordMetadata));
                    } else {
                        System.out.println("消息发送失败！" + String.valueOf(exception));
                        exception.printStackTrace();
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}
