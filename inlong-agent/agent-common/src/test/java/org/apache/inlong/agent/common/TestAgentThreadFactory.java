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

package org.apache.inlong.agent.common;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.inlong.agent.common.AgentThreadFactory.NAMED_THREAD_PLACEHOLDER;

public class TestAgentThreadFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestAgentThreadFactory.class);

    @Test
    public void testThreadRename() throws Exception {
        ExecutorService executor = Executors
                .newSingleThreadExecutor(new AgentThreadFactory("test"));
        Future<?> result = executor.submit(() -> {
            Assert.assertEquals("test-" + NAMED_THREAD_PLACEHOLDER + "-1", Thread.currentThread().getName());
            LOGGER.info("thread finished");
        });
        result.get();
    }
}
