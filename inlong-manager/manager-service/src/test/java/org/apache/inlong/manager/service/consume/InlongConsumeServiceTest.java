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

package org.apache.inlong.manager.service.consume;

import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.enums.ClusterType;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterRequest;
import org.apache.inlong.manager.pojo.common.OrderFieldEnum;
import org.apache.inlong.manager.pojo.common.OrderTypeEnum;
import org.apache.inlong.manager.pojo.consume.InlongConsumePageRequest;
import org.apache.inlong.manager.pojo.consume.pulsar.ConsumePulsarRequest;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.cluster.InlongClusterService;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.stream.InlongStreamService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Inlong consume service test
 */
public class InlongConsumeServiceTest extends ServiceBaseTest {

    String groupId = "consume_group_id";
    String streamId = "consume_group_id";
    String consumerGroup = "test_consumer_group";
    String clusterName = "consume_pulsar";
    String clusterTag = "consume_cluster";
    String adminUrl = "http://127.0.0.1:8080";
    String deadLetterTopic = "test_dlp";
    String tenant = "public";

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private InlongStreamService streamService;
    @Autowired
    private InlongConsumeService consumeService;
    @Autowired
    private InlongClusterService clusterService;

    @Test
    public void testAllProcess() {
        Integer consumeId = save();
        update(consumeId);
        get(consumeId);
        list();
        countStatus();
        delete(consumeId);
    }

    private Integer save() {
        saveGroup();
        saveStream();
        saveCluster();

        ConsumePulsarRequest request = new ConsumePulsarRequest();
        request.setId(1);
        request.setInlongGroupId(groupId);
        request.setInlongStreamId(streamId);
        request.setMqType(MQType.PULSAR);
        request.setTopic(streamId);
        request.setConsumerGroup(consumerGroup);
        request.setInCharges(GLOBAL_OPERATOR);
        request.setIsDlq(1);
        request.setDeadLetterTopic(deadLetterTopic);
        request.setIsRlq(0);
        request.setVersion(1);
        return consumeService.save(request, GLOBAL_OPERATOR);
    }

    private void saveGroup() {
        InlongPulsarInfo pulsarInfo = new InlongPulsarInfo();
        pulsarInfo.setInlongGroupId(groupId);
        pulsarInfo.setMqType(MQType.PULSAR);
        pulsarInfo.setCreator(GLOBAL_OPERATOR);
        pulsarInfo.setInCharges(GLOBAL_OPERATOR);
        pulsarInfo.setStatus(GroupStatus.CONFIG_SUCCESSFUL.getCode());
        pulsarInfo.setInlongClusterTag(clusterTag);
        pulsarInfo.setEnsemble(3);
        pulsarInfo.setWriteQuorum(3);
        pulsarInfo.setAckQuorum(2);
        groupService.save(pulsarInfo.genRequest(), GLOBAL_OPERATOR);
    }

    private void saveStream() {
        InlongStreamRequest streamRequest = new InlongStreamRequest();
        streamRequest.setInlongGroupId(groupId);
        streamRequest.setInlongStreamId(streamId);
        streamRequest.setDataEncoding("UTF-8");
        streamService.save(streamRequest, GLOBAL_OPERATOR);
    }

    private void saveCluster() {
        PulsarClusterRequest request = new PulsarClusterRequest();
        request.setClusterTags(clusterTag);
        request.setName(clusterName);
        request.setType(ClusterType.PULSAR);
        request.setAdminUrl(adminUrl);
        request.setTenant(tenant);
        request.setInCharges(GLOBAL_OPERATOR);
        clusterService.save(request, GLOBAL_OPERATOR);
    }

    private void list() {
        InlongConsumePageRequest request = new InlongConsumePageRequest();
        request.setPageNum(1);
        request.setPageSize(10);
        request.setOrderField(OrderFieldEnum.CREATE_TIME.name());
        request.setOrderType(OrderTypeEnum.DESC.name());
        request.setConsumerGroup(consumerGroup);
        consumeService.list(request);
    }

    private void countStatus() {
        consumeService.countStatus(GLOBAL_OPERATOR);
    }

    private void delete(Integer id) {
        consumeService.delete(id, GLOBAL_OPERATOR);
    }

    private void update(Integer id) {
        ConsumePulsarRequest request = new ConsumePulsarRequest();
        request.setId(id);
        request.setMqType(MQType.PULSAR);
        request.setInCharges("test_update");
        request.setVersion(1);
        request.setIsDlq(1);
        request.setDeadLetterTopic(deadLetterTopic);
        request.setIsRlq(0);
        request.setInlongGroupId(groupId);
        consumeService.update(request, GLOBAL_OPERATOR);
    }

    private void get(Integer id) {
        consumeService.get(id);
    }

}
