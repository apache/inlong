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

package org.apache.inlong.manager.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.manager.common.auth.SecretTokenAuthentication;
import org.apache.inlong.manager.common.pojo.group.InlongGroupRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class JsonUtilsTest {

    @Test
    public void testParseJson() {
        ObjectMapper objectMapper = new ObjectMapper();
        String json = "{\"dailyRecords\":10000000,\"description\":\"WeData_Offline\","
                + "\"enableZookeeper\":0,\"inCharges\":\"admin\","
                + "\"inlongGroupId\":\"f93c4cb10-a02a-4009-af95-d3143bb09901\","
                + "\"maxLength\":10000,\"middlewareType\":\"NONE\",\"mqType\":\"NONE\","
                + "\"name\":\"f93c4cb10-a02a-4009-af95-d3143bb09901\",\"peakRecords\":100000}\n";
        InlongGroupRequest request = null;
        try {
            request = objectMapper.readValue(json, InlongGroupRequest.class);
            System.out.println(request);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }

    @Test
    public void testParseAuthentication() {
        SecretTokenAuthentication authentication = new SecretTokenAuthentication("id", "key", "token");
        String authenticationStr = authentication.toString();
        Map<String, String> properties = JsonUtils.parseObject(authenticationStr,
                new TypeReference<Map<String, String>>() {
                });
        SecretTokenAuthentication newAuthentication = new SecretTokenAuthentication();
        newAuthentication.configure(properties);
        Assertions.assertTrue(authentication.getSecretId().equals(newAuthentication.getSecretId()));
        Assertions.assertTrue(authentication.getSecretKey().equals(newAuthentication.getSecretKey()));
        Assertions.assertTrue(authentication.getSToken().equals(newAuthentication.getSToken()));
    }
}
