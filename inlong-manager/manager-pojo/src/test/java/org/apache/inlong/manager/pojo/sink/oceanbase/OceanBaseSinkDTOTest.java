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

package org.apache.inlong.manager.pojo.sink.oceanbase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;

/**
 * Test for {@link OceanBaseSinkDTO}
 */
public class OceanBaseSinkDTOTest {

    @Test
    public void testFilterSensitive() throws Exception {
        // the sensitive params no use url code
        String originUrl = OceanBaseSinkDTO.filterSensitive(
                "jdbc:oceanbase://127.0.0.1,(allowLoadLocalInfile=yeſ,allowUrlInLocalInfile=yeſ,allowLoadLocalInfileInPath=.,maxAllowedPacket=655360),:3307/test");
        Assertions.assertEquals(
                "jdbc:oceanbase://127.0.0.1,(maxAllowedPacket=655360)(autoDeserialize=false,allowUrlInLocalInfile=false,allowLoadLocalInfile=false),:3307/test",
                originUrl);

        originUrl = OceanBaseSinkDTO.filterSensitive(
                "jdbc:oceanbase://127.0.0.1:3306?autoDeserialize=TRue&allowLoadLocalInfile = TRue&allowUrlInLocalInfile=TRue&allowLoadLocalInfileInPath=/&autoReconnect=true");
        Assertions.assertEquals(
                "jdbc:oceanbase://127.0.0.1:3306?autoReconnect=true&autoDeserialize=false&allowUrlInLocalInfile=false&allowLoadLocalInfile=false",
                originUrl);

        originUrl = OceanBaseSinkDTO.filterSensitive(
                "jdbc:oceanbase://127.0.0.1:3306?autoDeserialize=Yes&allowLoadLocalInfile = Yes&autoReconnect=true&allowUrlInLocalInfile=YEs&allowLoadLocalInfileInPath=/");
        Assertions.assertEquals(
                "jdbc:oceanbase://127.0.0.1:3306?autoReconnect=true&autoDeserialize=false&allowUrlInLocalInfile=false&allowLoadLocalInfile=false",
                originUrl);

        // the sensitive params use url code
        originUrl = OceanBaseSinkDTO.filterSensitive(
                URLEncoder.encode(
                        "jdbc:oceanbase://127.0.0.1:3306?autoDeserialize=TRue&allowLoadLocalInfile = TRue&allowUrlInLocalInfile=TRue&allowLoadLocalInfileInPath=/&autoReconnect=true",
                        "UTF-8"));
        Assertions.assertEquals(
                "jdbc:oceanbase://127.0.0.1:3306?autoReconnect=true&autoDeserialize=false&allowUrlInLocalInfile=false&allowLoadLocalInfile=false",
                originUrl);
    }

    @Test
    public void testGetFromRequestFiltersSensitiveParams() {
        // Regression test for the OceanBase sink JDBC URL sensitive-parameter bypass:
        // autoDeserialize=true must never survive getFromRequest(), since the resulting
        // DTO's jdbcUrl is persisted and later forwarded verbatim to the Flink JDBC connector.
        OceanBaseSinkRequest request = new OceanBaseSinkRequest();
        request.setJdbcUrl("jdbc:oceanbase://127.0.0.1:3306/test?autoDeserialize=true&allowLoadLocalInfile=true");

        OceanBaseSinkDTO dto = OceanBaseSinkDTO.getFromRequest(request, null);

        Assertions.assertFalse(dto.getJdbcUrl().contains("autoDeserialize=true"),
                "autoDeserialize=true must be neutralized before the OceanBase sink config is persisted");
        Assertions.assertTrue(dto.getJdbcUrl().contains("autoDeserialize=false"));
        Assertions.assertTrue(dto.getJdbcUrl().contains("allowLoadLocalInfile=false"));
    }

}
