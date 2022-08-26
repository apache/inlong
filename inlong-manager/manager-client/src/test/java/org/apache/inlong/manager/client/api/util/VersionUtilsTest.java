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

package org.apache.inlong.manager.client.api.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VersionUtilsTest {

    @Test
    void testCheckInlongVersion() {
        String current = "1.2.0";
        String target = "1.3.0";
        Assertions.assertFalse(VersionUtils.checkInlongVersion(current, target));
        current = "1.3.0";
        target = "1.3.0";
        Assertions.assertTrue(VersionUtils.checkInlongVersion(current, target));
        current = "1.3.5";
        target = "1.3.0";
        Assertions.assertTrue(VersionUtils.checkInlongVersion(current, target));
        current = "1.2.3-1";
        target = "1.3.0";
        try {
            VersionUtils.checkInlongVersion(current, target);
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof IllegalArgumentException);
            Assertions.assertTrue(e.getMessage().contains("Unsupported version"));
        }
    }
}
