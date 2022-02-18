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
import org.apache.inlong.manager.common.enums.EntityStatus;
import org.apache.inlong.manager.common.pojo.business.BusinessExtInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessPulsarInfo;
import org.apache.inlong.manager.dao.entity.BusinessExtEntity;
import org.apache.inlong.manager.dao.mapper.BusinessExtEntityMapper;
import org.apache.inlong.manager.web.WebBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;

import java.util.Arrays;
import java.util.List;

/**
 * Business service test
 */
@TestComponent
public class BusinessServiceTest extends WebBaseTest {

    private final String globalGroupId = "b_group1";
    private final String globalGroupName = "group1";
    private final String globalOperator = "test_user";

    @Autowired
    BusinessExtEntityMapper businessExtMapper;
    @Autowired
    private BusinessService businessService;

    public String saveBusiness(String groupName, String operator) {
        BusinessInfo businessInfo;
        try {
            businessInfo = businessService.get(globalGroupId);
            if (businessInfo != null) {
                return businessInfo.getInlongGroupId();
            }
        } catch (Exception e) {
            // ignore
        }

        businessInfo = new BusinessInfo();
        businessInfo.setName(groupName);
        businessInfo.setMiddlewareType(BizConstant.MIDDLEWARE_PULSAR);
        businessInfo.setCreator(operator);
        businessInfo.setInCharges(operator);
        businessInfo.setStatus(EntityStatus.BIZ_CONFIG_SUCCESSFUL.getCode());

        BusinessPulsarInfo pulsarInfo = new BusinessPulsarInfo();
        pulsarInfo.setMiddlewareType(BizConstant.MIDDLEWARE_PULSAR);
        pulsarInfo.setEnsemble(3);
        pulsarInfo.setWriteQuorum(3);
        pulsarInfo.setAckQuorum(2);

        businessInfo.setMqExtInfo(pulsarInfo);

        return businessService.save(businessInfo, operator);
    }

    @Test
    public void testSaveAndDelete() {
        String groupId = this.saveBusiness(globalGroupName, globalOperator);
        Assert.assertNotNull(groupId);

        boolean result = businessService.delete(groupId, globalOperator);
        Assert.assertTrue(result);
    }

    @Test
    public void testSaveAndUpdateExt() {
        // check insert
        BusinessExtInfo businessExtInfo1 = new BusinessExtInfo();
        businessExtInfo1.setId(1);
        businessExtInfo1.setInlongGroupId(globalGroupId);
        businessExtInfo1.setKeyName("pulsar_url");
        businessExtInfo1.setKeyValue("http://127.0.0.1:8080");

        BusinessExtInfo businessExtInfo2 = new BusinessExtInfo();
        businessExtInfo2.setId(2);
        businessExtInfo2.setInlongGroupId(globalGroupId);
        businessExtInfo2.setKeyName("pulsar_secret");
        businessExtInfo2.setKeyValue("QWEASDZXC");

        List<BusinessExtInfo> businessExtInfoList = Arrays.asList(businessExtInfo1, businessExtInfo2);
        businessService.saveOrUpdateExt(globalGroupId, businessExtInfoList);

        List<BusinessExtEntity> extEntityList = businessExtMapper.selectByGroupId(globalGroupId);
        Assert.assertEquals(2, extEntityList.size());
        Assert.assertEquals("pulsar_url", extEntityList.get(0).getKeyName());
        Assert.assertEquals("http://127.0.0.1:8080", extEntityList.get(0).getKeyValue());

        // check update
        businessExtInfo1.setKeyValue("http://127.0.0.1:8081");
        businessService.saveOrUpdateExt(globalGroupId, businessExtInfoList);
        extEntityList = businessExtMapper.selectByGroupId(globalGroupId);
        Assert.assertEquals(2, extEntityList.size());
        Assert.assertEquals("http://127.0.0.1:8081", extEntityList.get(0).getKeyValue());

        businessExtInfo2.setKeyValue("qweasdzxc");
        businessService.saveOrUpdateExt(globalGroupId, businessExtInfoList);
        extEntityList = businessExtMapper.selectByGroupId(globalGroupId);
        Assert.assertEquals(2, extEntityList.size());
        Assert.assertEquals("qweasdzxc", extEntityList.get(1).getKeyValue());
    }

}
