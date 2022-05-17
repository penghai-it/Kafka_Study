package it.ph.com.springboot_kafka_study;

import org.apache.kafka.clients.admin.*;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaAdminTest {
    public static final String TOPIC_NAME = "JAVA_TOPIC";

    //与kafka建立连接
    public static AdminClient initAdminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "123.57.214.53:9092");
        return AdminClient.create(properties);
    }

    //创建Topic
    @Test
    public void createTopicTest() {
        AdminClient adminClient = initAdminClient();
        //设置Topic的名字，分区数，备份数
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 5, (short) 1);
        CreateTopicsResult createTopicsResult = adminClient.createTopics(List.of(newTopic));
        try {
            //返回的是一个future类型（可以通过下面这个方式获取值），等到创建。成功不会有任何返回，失败则会报错
            createTopicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    //获取所有的topic（列举topic列表）
    @Test
    public void requestListTopicTest() {
        AdminClient adminClient = initAdminClient();
        //是否查看内部的topic，不是必须的可以不传
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);

        ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
        try {
            Set<String> topicNameSet = listTopicsResult.names().get();
            for (String topicName : topicNameSet) {
                System.err.println("TopicName： " + topicName);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    //删除topic
    @Test
    public void deleteTopic() {
        AdminClient adminClient = initAdminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("t2", "t1", "xdclass-topic", "phtest", "phtest-topic"));
        try {
            deleteTopicsResult.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    //查看topic详情
    @Test
    public void detailTopicTest() {
        AdminClient adminClient = initAdminClient();
        //需要查看的topic的name，可以传多个
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(List.of(TOPIC_NAME));
        try {
            Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
            Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();
            entries.forEach((entry) -> System.err.println("name: " + entry.getKey() + ",desc: " + entry.getValue()));
         /*   for (Map.Entry<String, TopicDescription> entry : entries) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }*/
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    //怎加某个Topic的分区数量
    @Test
    public void AddPartitionTest() {
        Map<String, NewPartitions> infoMap = new HashMap<>();
        AdminClient adminClient = initAdminClient();
        //新增的分区数量（注意：分区只能增加不能减少，因为减少数据就没有地方存放）
        NewPartitions newPartitions = NewPartitions.increaseTo(5);
        infoMap.put(TOPIC_NAME, newPartitions);
        CreatePartitionsResult partitions = adminClient.createPartitions(infoMap);
        try {
            partitions.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
