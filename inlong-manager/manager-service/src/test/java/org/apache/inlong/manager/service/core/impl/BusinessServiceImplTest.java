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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.inlong.manager.common.pojo.business.BusinessExtInfo;
import org.apache.inlong.manager.dao.entity.BusinessExtEntity;
import org.apache.inlong.manager.dao.mapper.BusinessExtEntityMapper;
import org.apache.inlong.manager.service.core.BaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class BusinessServiceImplTest extends BaseTest {

    @Autowired
    BusinessServiceImpl businessService;

    @Autowired
    BusinessExtEntityMapper businessExtMapper;

    @Test
    public void testSaveAndUpdateExt() {
        final String groupId = "group1";
        //check insert
        BusinessExtInfo businessExtInfo1 = new BusinessExtInfo();
        businessExtInfo1.setId(1);
        businessExtInfo1.setInlongGroupId("group1");
        businessExtInfo1.setKeyName("pulsar_url");
        businessExtInfo1.setKeyValue("http://127.0.0.1:8080");
        BusinessExtInfo businessExtInfo2 = new BusinessExtInfo();
        businessExtInfo2.setId(2);
        businessExtInfo2.setInlongGroupId("group1");
        businessExtInfo2.setKeyName("pulsar_secret");
        businessExtInfo2.setKeyValue("QWEASDZXC");
        ArrayList<BusinessExtInfo> businessExtInfoList = Lists.newArrayList(businessExtInfo1, businessExtInfo2);
        businessService.saveExt(groupId, businessExtInfoList);
        List<BusinessExtEntity> extEntityList = businessExtMapper.selectByGroupId(groupId);
        Assert.assertTrue(extEntityList.size() == 2);
        Assert.assertTrue(extEntityList.get(0).getKeyName().equals("pulsar_url"));
        Assert.assertTrue(extEntityList.get(0).getKeyValue().equals("http://127.0.0.1:8080"));
        //check update
        businessExtInfo1.setKeyValue("http://127.0.0.1:8081");
        businessService.updateExt(groupId,businessExtInfoList);
        extEntityList = businessExtMapper.selectByGroupId(groupId);
        Assert.assertTrue(extEntityList.size() == 2);
        Assert.assertTrue(extEntityList.get(0).getKeyValue().equals("http://127.0.0.1:8081"));
    }

}
