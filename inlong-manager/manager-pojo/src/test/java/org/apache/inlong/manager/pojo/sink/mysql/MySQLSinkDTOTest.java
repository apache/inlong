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

package org.apache.inlong.manager.pojo.sink.mysql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.inlong.manager.pojo.sink.mysql.MySQLSinkDTO.SENSITIVE_PARAM;
import static org.apache.inlong.manager.pojo.sink.mysql.MySQLSinkDTO.SYMBOL;

/**
 * Test for {@link MySQLSinkDTO}
 */
public class MySQLSinkDTOTest {

    @Test
    public void testFilterOther() {
        // the sensitive params at the first
        String originUrl = MySQLSinkDTO.filterSensitive(SENSITIVE_PARAM + SYMBOL + "autoReconnect=true");
        Assertions.assertEquals("autoReconnect=true", originUrl);

        // the sensitive params at the end
        originUrl = MySQLSinkDTO.filterSensitive("autoReconnect=true" + SYMBOL + SENSITIVE_PARAM);
        Assertions.assertEquals("autoReconnect=true", originUrl);

        // the sensitive params in the middle
        originUrl = MySQLSinkDTO.filterSensitive(
                "useSSL=false" + SYMBOL + SENSITIVE_PARAM + SYMBOL + "autoReconnect=true");
        Assertions.assertEquals("useSSL=false" + SYMBOL + "autoReconnect=true", originUrl);
    }

}
