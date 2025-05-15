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

package org.apache.inlong.sdk.dataproxy;

import org.apache.inlong.common.msg.AttributeConstants;
import org.apache.inlong.sdk.dataproxy.utils.ProxyUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ProxyUtilsTest {

    @Test
    public void getLocalIp() {
        String ip = ProxyUtils.getLocalIp();
        Assert.assertNotNull(ip);
    }

    @Test
    public void testGetValidAttrs() {
        Map<String, String> attrsMap = new HashMap<>();
        attrsMap.put("first", "       ");
        attrsMap.put("second", "");
        attrsMap.put("third", " stst");
        attrsMap.put("fourth", null);
        attrsMap.put("fifth", " fifth ");
        attrsMap.put("", "sixth");
        attrsMap.put(null, "seventh");
        attrsMap.put(null, "seventh");
        attrsMap.put("eighth", "eig&hth");
        attrsMap.put("ninth", "=ninth");
        attrsMap.put(AttributeConstants.DATA_TIME, "tenth");
        Map<String, String> tgtMap = ProxyUtils.getValidAttrs(attrsMap);
        Assert.assertNotNull(tgtMap);
        Assert.assertEquals(tgtMap.size(), 4);
        Assert.assertNotNull(tgtMap.get("first"));
        Assert.assertNotNull(tgtMap.get("second"));
        Assert.assertNotNull(tgtMap.get("third"));
        Assert.assertNull(tgtMap.get("fourth"));
        Assert.assertNotNull(tgtMap.get("fifth"));
        Assert.assertNull(tgtMap.get("eighth"));
        Assert.assertNull(tgtMap.get("ninth"));
        Assert.assertNull(tgtMap.get(AttributeConstants.DATA_TIME));
    }
}
