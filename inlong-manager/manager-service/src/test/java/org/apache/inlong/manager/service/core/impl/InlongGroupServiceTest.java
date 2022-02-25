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

package org.apache.inlong.manager.service.core.impl;

import org.apache.inlong.manager.common.enums.Constant;
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.pojo.group.InlongGroupExtInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPulsarInfo;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.apache.inlong.manager.dao.entity.InlongGroupExtEntity;
import org.apache.inlong.manager.dao.mapper.InlongGroupExtEntityMapper;
import org.apache.inlong.manager.service.core.InlongGroupService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;

import java.util.Arrays;
import java.util.List;

/**
 * Inlong group service test
 */
@TestComponent
public class InlongGroupServiceTest {

    private final String globalGroupId = "b_group1";
    private final String globalGroupName = "group1";
    private final String globalOperator = "test_user";

    @Autowired
    InlongGroupExtEntityMapper groupExtMapper;
    @Autowired
    private InlongGroupService groupService;

    /**
     * Test to save group
     */
    public String saveGroup(String groupName, String operator) {
        InlongGroupRequest groupInfo;
        try {
            groupInfo = groupService.get(globalGroupId);
            if (groupInfo != null) {
                return groupInfo.getInlongGroupId();
            }
        } catch (Exception e) {
            // ignore
        }

        groupInfo = new InlongGroupRequest();
        groupInfo.setName(groupName);
        groupInfo.setMiddlewareType(Constant.MIDDLEWARE_PULSAR);
        groupInfo.setCreator(operator);
        groupInfo.setInCharges(operator);
        groupInfo.setStatus(EntityStatus.GROUP_CONFIG_SUCCESSFUL.getCode());

        InlongGroupPulsarInfo pulsarInfo = new InlongGroupPulsarInfo();
        pulsarInfo.setMiddlewareType(Constant.MIDDLEWARE_PULSAR);
        pulsarInfo.setEnsemble(3);
        pulsarInfo.setWriteQuorum(3);
        pulsarInfo.setAckQuorum(2);

        groupInfo.setMqExtInfo(pulsarInfo);

        return groupService.save(groupInfo, operator);
    }

    @Test
    public void testSaveAndDelete() {
        String groupId = this.saveGroup(globalGroupName, globalOperator);
        Assert.assertNotNull(groupId);

        boolean result = groupService.delete(groupId, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testSaveAndUpdateExt() {
        // check insert
        InlongGroupExtInfo groupExtInfo1 = new InlongGroupExtInfo();
        groupExtInfo1.setId(1);
        groupExtInfo1.setInlongGroupId(globalGroupId);
        groupExtInfo1.setKeyName("pulsar_url");
        groupExtInfo1.setKeyValue("http://127.0.0.1:8080");

        InlongGroupExtInfo groupExtInfo2 = new InlongGroupExtInfo();
        groupExtInfo2.setId(2);
        groupExtInfo2.setInlongGroupId(globalGroupId);
        groupExtInfo2.setKeyName("pulsar_secret");
        groupExtInfo2.setKeyValue("QWEASDZXC");

        List<InlongGroupExtInfo> groupExtInfoList = Arrays.asList(groupExtInfo1, groupExtInfo2);
        groupService.saveOrUpdateExt(globalGroupId, groupExtInfoList);

        List<InlongGroupExtEntity> extEntityList = groupExtMapper.selectByGroupId(globalGroupId);
        Assert.assertEquals(2, extEntityList.size());
        Assert.assertEquals("pulsar_url", extEntityList.get(0).getKeyName());
        Assert.assertEquals("http://127.0.0.1:8080", extEntityList.get(0).getKeyValue());

        // check update
        groupExtInfo1.setKeyValue("http://127.0.0.1:8081");
        groupService.saveOrUpdateExt(globalGroupId, groupExtInfoList);
        extEntityList = groupExtMapper.selectByGroupId(globalGroupId);
        Assert.assertEquals(2, extEntityList.size());
        Assert.assertEquals("http://127.0.0.1:8081", extEntityList.get(0).getKeyValue());

        groupExtInfo2.setKeyValue("qweasdzxc");
        groupService.saveOrUpdateExt(globalGroupId, groupExtInfoList);
        extEntityList = groupExtMapper.selectByGroupId(globalGroupId);
        Assert.assertEquals(2, extEntityList.size());
        Assert.assertEquals("qweasdzxc", extEntityList.get(1).getKeyValue());
    }

}
