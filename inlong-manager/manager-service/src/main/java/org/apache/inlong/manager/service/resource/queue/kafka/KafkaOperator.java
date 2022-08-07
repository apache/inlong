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

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.conversion.ConversionHandle;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.service.cluster.InlongClusterServiceImpl;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Pulsar operator, supports creating topics and creating subscription.
 */
@Service
public class KafkaOperator {

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
    public void createTopic() throws InterruptedException, ExecutionException {
        AdminClient adminClient = adminClient();
        // topic的名称
        String name = "MyTopic3";
        // partition数量
        int numPartitions = 1;
        // 副本数量
        short replicationFactor = 1;
        NewTopic topic = new NewTopic(name, numPartitions, replicationFactor);
        CreateTopicsResult result = adminClient.createTopics(List.of(topic));
        // 避免客户端连接太快断开而导致Topic没有创建成功
        Thread.sleep(500);
        // 获取topic设置的partition数量
        System.out.println(result.numPartitions(name).get());
    }



    /**
     * Create Pulsar topic
     */
    public void createPartition() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        // 将MyTopic的Partition数量调整为2
        newPartitions.put("MyTopic", NewPartitions.increaseTo(2));
        CreatePartitionsResult result = adminClient.createPartitions(newPartitions);
        System.out.println(result.all().get());
    }

    /**
     * Force delete Pulsar topic
     */
    public void forceDeleteTopic(){
        AdminClient adminClient = adminClient();
        DeleteTopicsResult result = adminClient.deleteTopics(List.of("MyTopic1"));
        System.out.println(result.all().get());
    }
    public void forceDeletePartition(){}
    public boolean topicIsExists(){return true;}
    public boolean partitionIsExists(){return true;}
    // TODO: 2022/8/6  订阅关系



}
