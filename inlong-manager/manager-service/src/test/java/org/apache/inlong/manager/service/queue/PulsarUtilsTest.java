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

package org.apache.inlong.manager.service.queue;

import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.queue.pulsar.PulsarTopicMetadata;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for Pulsar utils.
 */
public class PulsarUtilsTest {

    private static final String DEFAULT_SERVICE_URL = "http://127.0.0.1:8080";

    private static final PulsarClusterInfo PULSAR_CLUSTER_INFO = new PulsarClusterInfo();

    private static final String DEFAULT_TENANT = "public";

    private static final String DEFAULT_NAMESPACE = "default";

    private static final String DEFAULT_TOPIC = "testtopic";

    private static final int DEFAULT_PARTITIONS_NUM = 3;

    private static final String DEFAULT_TOPIC_PATH = DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE + "/" + DEFAULT_TOPIC;

    private static RestTemplate restTemplate;

    @BeforeAll
    public static void before() {
        PULSAR_CLUSTER_INFO.setAdminUrl(DEFAULT_SERVICE_URL);
        restTemplate = new RestTemplate();
    }

    /**
     * Just using in local test.
     */
    @Disabled
    public void testListTenants() throws Exception {
        List<String> result = PulsarUtils.getPulsarTenants(restTemplate, PULSAR_CLUSTER_INFO);
        assertEquals(2, result.size());
    }

    /**
     * Just using in local test.
     */
    @Disabled
    public void testListNamespace() throws Exception {
        List<String> result = PulsarUtils.getPulsarNamespaces(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TENANT);
        assertEquals(1, result.size());
        assertEquals("public/default", result.get(0));
    }

    /**
     * Just using in local test.
     */
    @Disabled
    public void testGetPulsarTopics() throws Exception {
        List<String> result = PulsarUtils.getPulsarTopics(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
    }

    /**
     * Just using in local test.
     */
    @Disabled
    public void testGetPulsarPartitionedTopics() throws Exception {
        List<String> result = PulsarUtils.getPulsarPartitionedTopics(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
    }

    /**
     * Just using in local test.
     */
    @Disabled
    public void testCreateNonPartitionedTopic() throws Exception {
        List<String> topics = PulsarUtils.getPulsarTopics(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        String fullTopicName = "persistent://" + DEFAULT_TOPIC_PATH;
        if (topics.contains(fullTopicName)) {
            PulsarUtils.deleteNonPartitionedTopic(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TOPIC_PATH);
            topics = PulsarUtils.getPulsarTopics(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TENANT, DEFAULT_NAMESPACE);
            assertFalse(topics.contains(fullTopicName));
        }
        PulsarUtils.createNonPartitionedTopic(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TOPIC_PATH);
        topics = PulsarUtils.getPulsarTopics(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TENANT, DEFAULT_NAMESPACE);
        assertTrue(topics.contains(fullTopicName));
    }

    /**
     * Just using in local test.
     */
    @Disabled
    public void testCreatePartitionedTopic() throws Exception {
        List<String> topics = PulsarUtils.getPulsarPartitionedTopics(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        String fullTopicName = "persistent://" + DEFAULT_TOPIC_PATH;
        if (topics.contains(fullTopicName)) {
            PulsarUtils.forceDeletePartitionedTopic(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TOPIC_PATH);
            topics = PulsarUtils.getPulsarPartitionedTopics(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TENANT,
                    DEFAULT_NAMESPACE);
            assertFalse(topics.contains(fullTopicName));
        }
        PulsarUtils.createPartitionedTopic(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TOPIC_PATH,
                DEFAULT_PARTITIONS_NUM);
        topics = PulsarUtils.getPulsarPartitionedTopics(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        assertTrue(topics.contains(fullTopicName));
    }

    /**
     * Just using in local test.
     */
    @Disabled
    public void testGetPulsarStatsPartitionedTopics() throws Exception {
        String fullTopicName = "persistent://" + DEFAULT_TOPIC_PATH;
        List<String> topics = PulsarUtils.getPulsarPartitionedTopics(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TENANT,
                DEFAULT_NAMESPACE);
        if (topics.contains(fullTopicName)) {
            PulsarTopicMetadata metadata = PulsarUtils.getPulsarPartitionedTopicMetadata(restTemplate,
                    PULSAR_CLUSTER_INFO, DEFAULT_TOPIC_PATH);
            assertNotNull(metadata);
        }
    }

    /**
     * Just using in local test.
     */
    @Disabled
    public void testLookupPartitionedTopic() throws Exception {
        PulsarUtils.lookupPartitionedTopic(restTemplate, PULSAR_CLUSTER_INFO, DEFAULT_TOPIC_PATH);
    }

    /**
     * Just using in local test.
     */
    @Disabled
    public void testExamineMessage() throws Exception {
        String topicPath = DEFAULT_TENANT + "/" + DEFAULT_NAMESPACE + "/" + "testtopic-partition-1";
        ResponseEntity<byte[]> response = PulsarUtils.examineMessage(restTemplate, PULSAR_CLUSTER_INFO, topicPath,
                "latest", 1);
        if (200 == response.getStatusCodeValue()) {
            assertNotNull(response.getBody());
            assertNotNull(response.getHeaders());
        } else {
            assertNotNull(response.getHeaders());
        }
    }
}
