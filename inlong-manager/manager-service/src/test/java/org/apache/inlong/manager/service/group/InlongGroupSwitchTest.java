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

package org.apache.inlong.manager.service.group;

import org.apache.inlong.manager.common.consts.MQType;
import org.apache.inlong.manager.common.enums.GroupStatus;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.InlongStreamRequest;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class InlongGroupSwitchTest extends ServiceBaseTest {

    private final String globalGroupId = "group1";
    private final String globalStreamId = "stream1";
    private final String globalOperator = "admin";
    private final String backUpTag = "testBackUpTag";
    private final String backUpMqResource = "testBackUpMqResource";

    @Test
    public void testSwitchCluster() {
        String groupId = this.saveGroup(globalGroupId, globalOperator);
        Assertions.assertEquals(groupId, globalGroupId);
        InlongGroupInfo info = groupService.get(groupId);
        Assertions.assertEquals(info.getBackupInlongClusterTag(), backUpTag);
        Assertions.assertEquals(info.getBackupMqResource(), backUpMqResource);

        String newBackUpTag = "new back up tag";
        InlongGroupRequest request = info.genRequest();
        request.setBackupInlongClusterTag(newBackUpTag);
        String updateGroupId = groupService.update(request, globalOperator);
        Assertions.assertEquals(updateGroupId, groupId);
        InlongGroupInfo updateInfo = groupService.get(updateGroupId);
        Assertions.assertEquals(newBackUpTag, updateInfo.getBackupInlongClusterTag());
        Assertions.assertEquals(backUpMqResource, updateInfo.getBackupMqResource());
    }

    @Test
    public void testSwitchTopic() {
        this.saveGroup(globalGroupId, globalOperator);
        int id = this.saveInlongStream(globalGroupId, globalStreamId, globalOperator);
        InlongStreamInfo info = streamService.get(globalGroupId, globalStreamId);
        Assertions.assertEquals(backUpMqResource, info.getBackupMqResource());
        InlongStreamRequest request = info.genRequest();
        String newBackUpTopic = "new back up topic";
        request.setBackupMqResource(newBackUpTopic);
        Boolean updateResult = streamService.update(request, globalOperator);
        Assertions.assertTrue(updateResult);
        InlongStreamInfo updateInfo = streamService.get(globalGroupId, globalStreamId);
        Assertions.assertEquals(newBackUpTopic, updateInfo.getBackupMqResource());
    }

    public String saveGroup(String inlongGroupId, String operator) {
        InlongGroupInfo groupInfo;
        try {
            groupInfo = groupService.get(inlongGroupId);
            if (groupInfo != null) {
                return groupInfo.getInlongGroupId();
            }
        } catch (Exception e) {
            // ignore
        }

        InlongPulsarInfo pulsarInfo = new InlongPulsarInfo();
        pulsarInfo.setInlongGroupId(inlongGroupId);
        pulsarInfo.setMqType(MQType.PULSAR);
        pulsarInfo.setCreator(operator);
        pulsarInfo.setInCharges(operator);
        pulsarInfo.setStatus(GroupStatus.CONFIG_SUCCESSFUL.getCode());

        pulsarInfo.setEnsemble(3);
        pulsarInfo.setWriteQuorum(3);
        pulsarInfo.setAckQuorum(2);

        pulsarInfo.setBackupInlongClusterTag(backUpTag);
        pulsarInfo.setBackupMqResource(backUpMqResource);
        return groupService.save(pulsarInfo.genRequest(), operator);
    }

    private Integer saveInlongStream(String groupId, String streamId, String operator) {
        try {
            InlongStreamInfo response = streamService.get(groupId, streamId);
            if (response != null) {
                return response.getId();
            }
        } catch (Exception e) {
            // ignore
        }

        InlongStreamRequest request = new InlongStreamRequest();
        request.setInlongGroupId(groupId);
        request.setInlongStreamId(streamId);
        request.setDataEncoding("UTF-8");
        request.setBackupMqResource(backUpMqResource);

        return streamService.save(request, operator);
    }
}
