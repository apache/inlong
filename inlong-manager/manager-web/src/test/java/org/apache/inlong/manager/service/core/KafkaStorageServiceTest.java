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

package org.apache.inlong.manager.service.core;

import org.apache.inlong.manager.common.enums.BizConstant;
import org.apache.inlong.manager.common.pojo.datastorage.StorageResponse;
import org.apache.inlong.manager.common.pojo.datastorage.kafka.KafkaStorageRequest;
import org.apache.inlong.manager.common.pojo.datastorage.kafka.KafkaStorageResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.storage.StorageService;
import org.apache.inlong.manager.web.ServiceBaseTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Data storage service test
 */
public class KafkaStorageServiceTest extends ServiceBaseTest {

    @Autowired
    private StorageService storageService;
    @Autowired
    private DataStreamServiceTest streamServiceTest;

    private static final String globalGroupId = "b_group1";
    private static final String globalStreamId = "stream1";
    private static final String globalOperator = "test_user";
    private static final String bootstrapServers = "127.0.0.1:9092";
    private static final String consumerGroupId = "kafka_consumer_group_id";
    private static final String topicName = "kafka_topic_name";
    private static final String saslMechanism = "PLAIN";
    private static final String securityProtocol = "SASL_PLAINTEXT";
    private static final Long pollTimeoutMs = 1000L;

    private static Integer kafkaStorageId;

    @Before
    public void saveStorage() {
        streamServiceTest.saveDataStream(globalGroupId, globalStreamId, globalOperator);
        KafkaStorageRequest storageInfo = new KafkaStorageRequest();
        storageInfo.setInlongGroupId(globalGroupId);
        storageInfo.setInlongStreamId(globalStreamId);
        storageInfo.setStorageType(BizConstant.STORAGE_KAFKA);
        storageInfo.setGroupId(consumerGroupId);
        storageInfo.setBootstrapServers(bootstrapServers);
        storageInfo.setTopicName(topicName);
        storageInfo.setSaslMechanism(saslMechanism);
        storageInfo.setSecurityProtocol(securityProtocol);
        storageInfo.setPollTimeoutMs(pollTimeoutMs);
        storageInfo.setEnableCreateResource(BizConstant.DISABLE_CREATE_RESOURCE);
        kafkaStorageId = storageService.save(storageInfo, globalOperator);
    }

    @After
    public void deleteKafkaStorage() {
        boolean result = storageService.delete(kafkaStorageId, BizConstant.STORAGE_KAFKA, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        StorageResponse storage = storageService.get(kafkaStorageId, BizConstant.STORAGE_KAFKA);
        Assert.assertEquals(globalGroupId, storage.getInlongGroupId());
    }

    @Test
    public void testGetAndUpdate() {
        StorageResponse response = storageService.get(kafkaStorageId, BizConstant.STORAGE_KAFKA);
        Assert.assertEquals(globalGroupId, response.getInlongGroupId());

        KafkaStorageResponse kafkaStorageResponse = (KafkaStorageResponse) response;
        kafkaStorageResponse.setEnableCreateResource(BizConstant.ENABLE_CREATE_RESOURCE);

        KafkaStorageRequest request = CommonBeanUtils.copyProperties(kafkaStorageResponse, KafkaStorageRequest::new);
        boolean result = storageService.update(request, globalOperator);
        Assert.assertTrue(result);
    }

}
