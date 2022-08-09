package org.apache.inlong.manager.service.resource.queue.kafka;

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.pojo.cluster.kafka.KafkaClusterInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.pulsar.client.admin.PulsarAdmin;

/**
 * Pulsar connection utils
 */
@Slf4j
public class KafkaUtils {
  public static AdminClient getAdminClient(KafkaClusterInfo kafkaClusterInfo){
    Properties properties = new Properties();
    // 配置Kafka服务的访问地址及端口号
    properties.setProperty(AdminClientConfig.
        BOOTSTRAP_SERVERS_CONFIG, kafkaClusterInfo.getBootStrapServers());
    // 创建AdminClient实例
    return AdminClient.create(properties);
  }

  public static KafkaConsumer createKafkaConsumer(KafkaClusterInfo kafkaClusterInfo){
    Properties properties = new Properties();
    // 连接的 kafka 集群地址
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaClusterInfo.getBootStrapServers());
    // 消费者分组
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaClusterInfo.getGroupId());
    //确认自动提交
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    // 自动提交间隔
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    // 序列化
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.IntegerDeserializer");
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    //对于不同的groupid保证能消费到之前的消息，重置offset
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return new KafkaConsumer(properties);
  }
}
