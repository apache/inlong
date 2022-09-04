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

package org.apache.inlong.manager.service.core.consume;

import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.pojo.consume.InlongConsumeInfo;
import org.apache.inlong.manager.pojo.consume.pulsar.ConsumePulsarRequest;
import org.apache.inlong.manager.service.consume.InlongConsumeService;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;

/**
 * Inlong consume service test
 */
@TestComponent
public class InlongConsumeServiceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(InlongConsumeServiceTest.class);

    String groupId = "consume_service_test_group";
    String streamId = "consume_service_test_stream";
    String consumerGroup = "test_consumer_group";
    String operator = "admin";

    @Autowired
    private InlongStreamServiceTest streamServiceTest;
    @Autowired
    private InlongConsumeService consumeService;

    /**
     * Test save inlong consume
     */
    public Integer saveInlongConsume(Integer consumeId, String groupId, String streamId, String operator) {
        try {
            InlongConsumeInfo consumeInfo = consumeService.get(consumeId);
            if (consumeInfo != null) {
                return consumeInfo.getId();
            }
        } catch (Exception e) {
            // ignore
        }

        streamServiceTest.saveInlongStream(groupId, streamId, operator);

        ConsumePulsarRequest request = new ConsumePulsarRequest();
        request.setInlongGroupId(groupId);
        request.setInlongStreamId(streamId);
        request.setMqType(MQType.PULSAR);
        request.setConsumerGroup(consumerGroup);
        request.setInCharges("admin");
        request.setIsDlq(1);
        request.setDeadLetterTopic("test_dlp");
        request.setIsRlq(0);

        return consumeService.save(request, operator);
    }

    // @Test
    public void testSaveAndDelete() {
        Integer consumeId = 1;
        Integer id = this.saveInlongConsume(consumeId, streamId, groupId, operator);
        Assertions.assertNotNull(id);

        boolean result = consumeService.delete(id, operator);
        Assertions.assertTrue(result);
    }

    @Test
    public void test() {
        LOGGER.info("If you don't add test, UnusedImports: Unused import: org.junit.Test.");
    }

}
