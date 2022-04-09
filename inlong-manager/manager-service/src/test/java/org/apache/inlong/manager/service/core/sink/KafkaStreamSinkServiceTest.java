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

package org.apache.inlong.manager.service.core.sink;

import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.SinkType;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkRequest;
import org.apache.inlong.manager.common.pojo.sink.kafka.KafkaSinkResponse;
import org.apache.inlong.manager.common.util.CommonBeanUtils;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.impl.InlongStreamServiceTest;
import org.apache.inlong.manager.service.sink.StreamSinkService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Stream sink service test
 */
public class KafkaStreamSinkServiceTest extends ServiceBaseTest {

    private static final String globalGroupId = "b_group1";
    private static final String globalStreamId = "stream1";
    private static final String globalOperator = "test_user";
    private static final String bootstrapServers = "127.0.0.1:9092";
    private static final String serializationType = "Json";
    private static final String topicName = "kafka_topic_name";
    private static final String sinkName = "default";
    private static Integer kafkaSinkId;

    @Autowired
    private StreamSinkService sinkService;
    @Autowired
    private InlongStreamServiceTest streamServiceTest;

    @Before
    public void saveSink() {
        streamServiceTest.saveInlongStream(globalGroupId, globalStreamId, globalOperator);
        KafkaSinkRequest sinkInfo = new KafkaSinkRequest();
        sinkInfo.setInlongGroupId(globalGroupId);
        sinkInfo.setInlongStreamId(globalStreamId);
        sinkInfo.setSinkType(SinkType.SINK_KAFKA);
        sinkInfo.setSinkName(sinkName);
        sinkInfo.setSerializationType(serializationType);
        sinkInfo.setBootstrapServers(bootstrapServers);
        sinkInfo.setTopicName(topicName);
        sinkInfo.setEnableCreateResource(Constant.DISABLE_CREATE_RESOURCE);
        kafkaSinkId = sinkService.save(sinkInfo, globalOperator);
    }

    @After
    public void deleteKafkaSink() {
        boolean result = sinkService.delete(kafkaSinkId, SinkType.SINK_KAFKA, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testListByIdentifier() {
        SinkResponse sink = sinkService.get(kafkaSinkId, SinkType.SINK_KAFKA);
        Assert.assertEquals(globalGroupId, sink.getInlongGroupId());
    }

    @Test
    public void testGetAndUpdate() {
        SinkResponse response = sinkService.get(kafkaSinkId, SinkType.SINK_KAFKA);
        Assert.assertEquals(globalGroupId, response.getInlongGroupId());

        KafkaSinkResponse kafkaSinkResponse = (KafkaSinkResponse) response;
        kafkaSinkResponse.setEnableCreateResource(Constant.ENABLE_CREATE_RESOURCE);

        KafkaSinkRequest request = CommonBeanUtils.copyProperties(kafkaSinkResponse, KafkaSinkRequest::new);
        boolean result = sinkService.update(request, globalOperator);
        Assert.assertTrue(result);
    }

}
