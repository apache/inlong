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

package org.apache.inlong.audit.utils;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DataUtilsTest {

    @Test
    public void testIsDataTimeValid() {
        long deviation = 604800000;
        long dataTime = System.currentTimeMillis();
        boolean valid = DataUtils.isDataTimeValid(dataTime, deviation);
        assertTrue(valid);

        dataTime = System.currentTimeMillis() - 1000 * 60 * 60 * 24 * 2;
        valid = DataUtils.isDataTimeValid(dataTime, deviation);
        assertTrue(valid);

        dataTime = System.currentTimeMillis() + 1000 * 60 * 60 * 24 * 2;
        valid = DataUtils.isDataTimeValid(dataTime, deviation);
        assertTrue(valid);

        dataTime = System.currentTimeMillis() - 1000 * 60 * 60 * 24 * 8;
        valid = DataUtils.isDataTimeValid(dataTime, deviation);
        assertFalse(valid);

        dataTime = System.currentTimeMillis() + 1000 * 60 * 60 * 24 * 8;
        valid = DataUtils.isDataTimeValid(dataTime, deviation);
        assertFalse(valid);

        dataTime = 1734356619540000L;
        valid = DataUtils.isDataTimeValid(dataTime, deviation);
        assertFalse(valid);

        dataTime = 1L;
        valid = DataUtils.isDataTimeValid(dataTime, deviation);
        assertFalse(valid);
    }

    @Test
    public void testIsAuditItemValid() {
        String auditItem = null;
        boolean valid = DataUtils.isAuditItemValid(auditItem);
        assertTrue(valid);

        auditItem = "";
        valid = DataUtils.isAuditItemValid(auditItem);
        assertTrue(valid);

        auditItem = "1@dff";
        valid = DataUtils.isAuditItemValid(auditItem);
        assertTrue(valid);

        auditItem = "fb320c7e51";
        valid = DataUtils.isAuditItemValid(auditItem);
        assertTrue(valid);

        auditItem = "127.0.0.1";
        valid = DataUtils.isAuditItemValid(auditItem);
        assertTrue(valid);

        Random random = new Random();
        StringBuilder stringBuilder128 = new StringBuilder(128);
        for (int i = 0; i < 128; i++) {
            char c = (char) (random.nextInt(26) + 'a');
            stringBuilder128.append(c);
        }
        valid = DataUtils.isAuditItemValid(stringBuilder128.toString());
        assertTrue(valid);

        StringBuilder stringBuilder256 = new StringBuilder(256);
        for (int i = 0; i < 256; i++) {
            char c = (char) (random.nextInt(26) + 'a');
            stringBuilder256.append(c);
        }
        valid = DataUtils.isAuditItemValid(stringBuilder256.toString());
        assertFalse(valid);

    }
}