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

package org.apache.inlong.manager.service.thirdpart.mq.util;

import com.google.common.collect.Lists;
import java.lang.reflect.Field;
import java.util.ArrayList;
import org.apache.inlong.manager.common.pojo.business.BusinessExtInfo;
import org.apache.inlong.manager.common.pojo.business.BusinessInfo;
import org.apache.inlong.manager.common.settings.BusinessSettings;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;

public class PulsarUtilsTest {

    @Test
    public void testGetPulsarAdmin() {
        BusinessExtInfo businessExtInfo1 = new BusinessExtInfo();
        businessExtInfo1.setId(1);
        businessExtInfo1.setInlongGroupId("group1");
        businessExtInfo1.setKeyName(BusinessSettings.PULSAR_ADMIN_URL);
        businessExtInfo1.setKeyValue("http://127.0.0.1:8080");
        BusinessExtInfo businessExtInfo2 = new BusinessExtInfo();
        businessExtInfo2.setId(2);
        businessExtInfo2.setInlongGroupId("group1");
        businessExtInfo2.setKeyName(BusinessSettings.PULSAR_AUTHENTICATION);
        businessExtInfo2.setKeyValue("QWEASDZXC");
        ArrayList<BusinessExtInfo> businessExtInfoList = Lists.newArrayList(businessExtInfo1, businessExtInfo2);
        BusinessInfo businessInfo = new BusinessInfo();
        businessInfo.setExtList(businessExtInfoList);
        final String defaultServiceUrl = "http://127.0.0.1:10080";
        try {
            PulsarAdmin admin = PulsarUtils.getPulsarAdmin(businessInfo, defaultServiceUrl);
            Assert.assertTrue(admin.getServiceUrl().equals("http://127.0.0.1:8080"));
            Field auth = ReflectionUtils.findField(PulsarAdminImpl.class, "auth");
            auth.setAccessible(true);
            Authentication authentication = (Authentication) auth.get(admin);
            Assert.assertTrue(authentication != null);

            businessExtInfoList = new ArrayList<>();
            businessInfo.setExtList(businessExtInfoList);
            admin = PulsarUtils.getPulsarAdmin(businessInfo, defaultServiceUrl);
            Assert.assertTrue(admin.getServiceUrl().equals("http://127.0.0.1:10080"));
            auth = ReflectionUtils.findField(PulsarAdminImpl.class, "auth");
            auth.setAccessible(true);
            authentication = (Authentication) auth.get(admin);
            Assert.assertTrue(authentication instanceof AuthenticationDisabled);
        } catch (PulsarClientException e) {
            Assert.fail();
        } catch (IllegalAccessException e) {
            Assert.fail();
        }
    }

}
