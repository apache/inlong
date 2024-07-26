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

package org.apache.inlong.manager.service.resource.sink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sun.misc.BASE64Encoder;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;

public class Base64CompatibleTest {

    @Test
    public void base64CompatibleForDifferentVersionTest() {
        String tokenStr = "root" + ":" + "admin";
        byte[] tokeBytes = tokenStr.getBytes(StandardCharsets.UTF_8);
        // token encode by sun.misc.BASE64Encoder, this test will fail if the jdk version is 9 or later
        // so, this test should be removed when Apache InLong project support jdk 9 or later
        String tokenOld = String.valueOf(new BASE64Encoder().encode(tokeBytes));
        // token encode by java.util.Base64.Encoder
        Encoder encoder = Base64.getEncoder();
        String tokenNew = encoder.encodeToString(tokeBytes);
        Assertions.assertEquals(tokenOld, tokenNew);
    }
}
