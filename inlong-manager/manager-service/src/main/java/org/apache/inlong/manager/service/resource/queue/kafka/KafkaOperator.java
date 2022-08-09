/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.service.resource.queue.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.inlong.manager.common.conversion.ConversionHandle;
import org.apache.inlong.manager.pojo.cluster.kafka.KafkaClusterInfo;
import org.apache.inlong.manager.service.cluster.InlongClusterServiceImpl;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Pulsar operator, supports creating topics and creating subscription.
 */
@Service
public class KafkaOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(InlongClusterServiceImpl.class);


  @Autowired
  private ConversionHandle conversionHandle;

  /**
   * 配置并创建AdminClient
   */
  public static AdminClient adminClient() {
    Properties properties = new Properties();
    // 配置Kafka服务的访问地址及端口号
    properties.setProperty(AdminClientConfig.
        BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    // 创建AdminClient实例
    return AdminClient.create(properties);
  }

  /**
   * Create Pulsar namespace
   */
  public void createTopic(KafkaClusterInfo kafkaClusterInfo)
      throws InterruptedException, ExecutionException {
    AdminClient adminClient = KafkaUtils.getAdminClient(kafkaClusterInfo);
    NewTopic topic = new NewTopic(kafkaClusterInfo.getTopicName(),
        kafkaClusterInfo.getNumPartitions(),
        kafkaClusterInfo.getReplicationFactor());
    CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(topic));
    // 避免客户端连接太快断开而导致Topic没有创建成功
    Thread.sleep(500);
    LOGGER.info("success to create kafka topic={}, with={} numPartitions",
        kafkaClusterInfo.getTopicName(),
        result.numPartitions(kafkaClusterInfo.getTopicName()).get());
  }

//  /**
//   * Create Pulsar topic
//   */
//  public void createPartition() throws ExecutionException, InterruptedException {
//    AdminClient adminClient = adminClient();
//    Map<String, NewPartitions> newPartitions = new HashMap<>();
//    // 将MyTopic的Partition数量调整为2
//    newPartitions.put("MyTopic", NewPartitions.increaseTo(2));
//    CreatePartitionsResult result = adminClient.createPartitions(newPartitions);
//    System.out.println(result.all().get());
//  }

  /**
   * Force delete Pulsar topic
   */
  public void forceDeleteTopic(KafkaClusterInfo kafkaClusterInfo, String topicName) {
    AdminClient adminClient = KafkaUtils.getAdminClient(kafkaClusterInfo);
    DeleteTopicsResult result = adminClient.deleteTopics(Collections.singletonList(topicName));
    LOGGER.info("success to delete topic={}", topicName);
//    System.out.println(result.all().get());
  }

  public boolean topicIsExists(String topic) throws ExecutionException, InterruptedException {
    AdminClient adminClient = adminClient();
    Set<String> topicList = adminClient.listTopics().names().get();
    return topicList.contains(topic);
  }

  public void createSubscriptions(KafkaClusterInfo kafkaClusterInfo, String topic) {
    KafkaConsumer kafkaConsumer = KafkaUtils.createKafkaConsumer(kafkaClusterInfo);
    //订阅
    kafkaConsumer.subscribe(Collections.singletonList(topic));
  }

  public boolean subscriptionIsExists(KafkaClusterInfo kafkaClusterInfo, String topic) {
    KafkaConsumer consumer = KafkaUtils.createKafkaConsumer(kafkaClusterInfo);
    try {
      Map<String, List<PartitionInfo>> topics = consumer.listTopics();
      List<PartitionInfo> partitions = topics.get(topic);
      if (partitions == null) {
        LOGGER.info("subscription is exit");
        return false;
      }
      return true;
    } finally {
      consumer.close();
    }

  }

}
