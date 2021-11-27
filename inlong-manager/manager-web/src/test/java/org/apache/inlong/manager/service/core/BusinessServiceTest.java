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
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessPulsarInfo;
import org.apache.inlong.manager.web.ServiceBaseTest;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestComponent;

/**
 * Business Service Test
 */
@TestComponent
public class BusinessServiceTest extends ServiceBaseTest {

    @Autowired
    private BusinessService businessService;

    public String saveBusiness(String groupName, String operator) {
        BusinessInfo businessInfo = new BusinessInfo();
        businessInfo.setName(groupName);
        businessInfo.setMiddlewareType(BizConstant.MIDDLEWARE_PULSAR);
        businessInfo.setCreator(operator);

        BusinessPulsarInfo pulsarInfo = new BusinessPulsarInfo();
        pulsarInfo.setMiddlewareType(BizConstant.MIDDLEWARE_PULSAR);
        pulsarInfo.setEnsemble(3);
        pulsarInfo.setWriteQuorum(3);
        pulsarInfo.setAckQuorum(2);

        businessInfo.setMqExtInfo(pulsarInfo);

        return businessService.save(businessInfo, operator);
    }

    @Test
    public void testSave() {
        String groupName = "test_group1";
        String operator = "test_user";
        String groupId = this.saveBusiness(groupName, operator);
        Assert.assertNotNull(groupId);
    }

    @Test
    public void testDelete() {
        String groupName = "test_group2";
        String operator = "test_user";
        String groupId = this.saveBusiness(groupName, operator);
        boolean result = businessService.delete(groupId, operator);
        Assert.assertTrue(result);
    }

}
