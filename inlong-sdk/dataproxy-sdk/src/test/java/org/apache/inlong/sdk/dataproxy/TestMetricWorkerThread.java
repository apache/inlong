/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sdk.dataproxy;

import java.util.concurrent.TimeUnit;

import org.apache.inlong.sdk.dataproxy.network.Utils;
import org.apache.inlong.sdk.dataproxy.threads.MetricWorkerThread;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMetricWorkerThread {

    private static MetricWorkerThread workerThread;

    @BeforeClass
    public static void setup() throws Exception {
        ProxyClientConfig config = new ProxyClientConfig(Utils.getLocalIp(),
                true, "127.0.0.1", 8099, "test", "all");
        workerThread = new MetricWorkerThread(config, null);
        workerThread.start();
    }

    @Test
    public void testMetricCount() throws Exception {
        for (int i = 0; i < 10000; i++) {
            workerThread.recordNumByKey(String.valueOf(i), "test", "test1", "127.0.0.1",
                    System.currentTimeMillis(), System.currentTimeMillis(), i);
        }
        TimeUnit.SECONDS.sleep(59);
        for (int i = 0; i < 2000; i++) {
            workerThread.recordSuccessByMessageId(String.valueOf(i));
        }
        for (int i = 2000; i < 2060; i++) {
            workerThread.recordFailedByMessageId(String.valueOf(i));
        }
        TimeUnit.SECONDS.sleep(10);
    }

    @Test
    public void getLocalIp() {
        String ip = Utils.getLocalIp();
        System.out.println(ip);
    }

    @AfterClass
    public static void teardown() {
        workerThread.close();
    }

}
