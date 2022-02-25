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
import org.apache.inlong.manager.common.pojo.consumption.ConsumptionInfo;
import org.apache.inlong.manager.common.pojo.consumption.ConsumptionPulsarInfo;
import org.apache.inlong.manager.service.ServiceBaseTest;
import org.apache.inlong.manager.service.core.ConsumptionService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Consumption service test
 */
public class ConsumptionServiceTest extends ServiceBaseTest {

    @Autowired
    private ConsumptionService consumptionService;
    @Autowired
    private InlongGroupServiceTest groupServiceTest;

    private Integer saveConsumption(String inlongGroup, String consumerGroup, String operator) {
        ConsumptionInfo consumptionInfo = new ConsumptionInfo();
        consumptionInfo.setTopic(inlongGroup);
        consumptionInfo.setConsumerGroupName(consumerGroup);
        consumptionInfo.setInlongGroupId("b_" + inlongGroup);
        consumptionInfo.setMiddlewareType(Constant.MIDDLEWARE_PULSAR);
        consumptionInfo.setCreator(operator);

        ConsumptionPulsarInfo pulsarInfo = new ConsumptionPulsarInfo();
        pulsarInfo.setMiddlewareType(Constant.MIDDLEWARE_PULSAR);
        pulsarInfo.setIsDlq(1);
        pulsarInfo.setDeadLetterTopic("test_dlq");
        pulsarInfo.setIsRlq(0);

        consumptionInfo.setMqExtInfo(pulsarInfo);

        return consumptionService.save(consumptionInfo, operator);
    }

    @Test
    public void testSave() {
        String inlongGroup = "inlong_group1";
        String consumerGroup = "test_save_consumer_group";
        String operator = "test_user";
        groupServiceTest.saveGroup(inlongGroup, operator);
        Integer id = this.saveConsumption(inlongGroup, consumerGroup, operator);
        Assert.assertNotNull(id);
    }

    @Test
    public void testDelete() {
        String inlongGroup = "inlong_group2";
        String operator = "test_user";
        String consumerGroup = "test_delete_consumer_group";
        groupServiceTest.saveGroup(inlongGroup, operator);
        Integer id = this.saveConsumption(inlongGroup, consumerGroup, operator);
        boolean result = consumptionService.delete(id, operator);
        Assert.assertTrue(result);
    }

}
