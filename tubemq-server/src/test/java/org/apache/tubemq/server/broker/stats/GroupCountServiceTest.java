/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.server.broker.stats;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/***
 * GroupCountService test.
 */
public class GroupCountServiceTest {

    @Test
    public void add() {
        GroupCountService groupCountService = new GroupCountService("PutCounterGroup", "Producer", 60 * 1000);
        groupCountService.add("key", 1L, 100);
        Map<String, CountItem> items = new HashMap<>();
        items.put("key1", new CountItem(1L, 1024));
        items.put("key2", new CountItem(1L, 1024));
        // add counts
        groupCountService.add(items);
    }
}
