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

package org.apache.inlong.sort.protocol.source;

import java.io.IOException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.protocol.FieldInfo;
import org.apache.inlong.sort.protocol.deserialization.DeserializationInfo;
import org.apache.inlong.sort.protocol.deserialization.TDMsgCsvDeserializationInfo;
import org.junit.Assert;
import org.junit.Test;

public class PulsarSourceInfoTest {

    @Test
    public void testSerializeAndDeserialize() {
        FieldInfo[] pulsarFields = new FieldInfo[]{
                new FieldInfo("f1", StringFormatInfo.INSTANCE),
                new FieldInfo("f2", StringFormatInfo.INSTANCE)
        };
        DeserializationInfo deserializationInfo = new TDMsgCsvDeserializationInfo("stream", ',');

        PulsarSourceInfo pulsarSourceInfo = new PulsarSourceInfo(
                "http://127.0.0.1:8080",
                "pulsar://127.0.0.1:6650",
                "business",
                "consumer",
                deserializationInfo,
                pulsarFields,
                null
        );
        final ObjectMapper objectMapper = new ObjectMapper();
        try {
            byte[] values = objectMapper.writeValueAsBytes(pulsarSourceInfo);
            pulsarSourceInfo = objectMapper.readValue(values, PulsarSourceInfo.class);
            Assert.assertTrue(pulsarSourceInfo.getAdminUrl().equals("http://127.0.0.1:8080"));
            Assert.assertTrue(pulsarSourceInfo.getServiceUrl().equals("pulsar://127.0.0.1:6650"));
            Assert.assertTrue(pulsarSourceInfo.getTopic().equals("business"));
            Assert.assertTrue(pulsarSourceInfo.getSubscriptionName().equals("consumer"));
            Assert.assertTrue(pulsarSourceInfo.getDeserializationInfo() instanceof TDMsgCsvDeserializationInfo);
            Assert.assertTrue(pulsarSourceInfo.getAuthentication() == null);
        } catch (JsonProcessingException e) {
            Assert.fail();
        } catch (IOException e) {
            Assert.fail();
        }

    }

}
