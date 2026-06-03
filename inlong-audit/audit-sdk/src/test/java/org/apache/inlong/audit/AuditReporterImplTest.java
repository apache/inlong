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

package org.apache.inlong.audit;

import org.apache.inlong.audit.util.StatInfo;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.inlong.audit.AuditIdEnum.AGENT_INPUT;
import static org.apache.inlong.audit.AuditIdEnum.SORT_HIVE_INPUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class AuditReporterImplTest {

    @Test
    public void TestBuildAuditId() {
        int auditId = AuditOperator.getInstance().buildSuccessfulAuditId(AGENT_INPUT);
        assertEquals(3, auditId);
        auditId = AuditOperator.getInstance().buildFailedAuditId(AGENT_INPUT);
        assertEquals(524291, auditId);
        auditId = AuditOperator.getInstance().buildRetryAuditId(AGENT_INPUT);
        assertEquals(65539, auditId);
        auditId = AuditOperator.getInstance().buildDiscardAuditId(AGENT_INPUT);
        assertEquals(131075, auditId);

        auditId = AuditOperator.getInstance().buildSuccessfulAuditId(SORT_HIVE_INPUT, false);
        assertEquals(262151, auditId);
        auditId = AuditOperator.getInstance().buildFailedAuditId(SORT_HIVE_INPUT, false);
        assertEquals(786439, auditId);
        auditId = AuditOperator.getInstance().buildDiscardAuditId(SORT_HIVE_INPUT, false);
        assertEquals(393223, auditId);
        auditId = AuditOperator.getInstance().buildRetryAuditId(SORT_HIVE_INPUT, false);
        assertEquals(327687, auditId);
    }

    /**
     * Invoke the private addByKey method via reflection so that its behavior can be tested directly.
     */
    private static void invokeAddByKey(AuditReporterImpl reporter, long isolateKey,
            String statKey, long count, long size, long delayTime) throws Exception {
        Method method = AuditReporterImpl.class.getDeclaredMethod(
                "addByKey", long.class, String.class, long.class, long.class, long.class);
        method.setAccessible(true);
        method.invoke(reporter, isolateKey, statKey, count, size, delayTime);
    }

    /**
     * Get the preStatMap via reflection so that the internal state can be verified in tests.
     */
    @SuppressWarnings("unchecked")
    private static ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>> getPreStatMap(
            AuditReporterImpl reporter) throws Exception {
        Field field = AuditReporterImpl.class.getDeclaredField("preStatMap");
        field.setAccessible(true);
        return (ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>>) field.get(reporter);
    }

    /**
     * Verify that on the first invocation the inner map and StatInfo are created correctly,
     * and count/size/delay are accumulated on top of 0.
     */
    @Test
    public void testAddByKey_firstInsert() throws Exception {
        AuditReporterImpl reporter = new AuditReporterImpl();
        long isolateKey = 1L;
        String statKey = "1700000000:groupA:streamA:1:tagA:-1";

        invokeAddByKey(reporter, isolateKey, statKey, 10L, 100L, 5L);

        ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>> preStatMap = getPreStatMap(reporter);
        assertNotNull("preStatMap should not be null", preStatMap);
        assertNotNull("inner map should be created", preStatMap.get(isolateKey));
        StatInfo stat = preStatMap.get(isolateKey).get(statKey);
        assertNotNull("StatInfo should be created", stat);
        assertEquals(10L, stat.count.get());
        assertEquals(100L, stat.size.get());
        assertEquals(5L, stat.delay.get());
    }

    /**
     * Verify that count/size/delay are accumulated strictly when addByKey is called multiple
     * times on the same (isolateKey, statKey).
     */
    @Test
    public void testAddByKey_accumulateOnSameKey() throws Exception {
        AuditReporterImpl reporter = new AuditReporterImpl();
        long isolateKey = 1L;
        String statKey = "logtime:group:stream:auditId:tag:0";

        invokeAddByKey(reporter, isolateKey, statKey, 1L, 10L, 100L);
        invokeAddByKey(reporter, isolateKey, statKey, 2L, 20L, 200L);
        invokeAddByKey(reporter, isolateKey, statKey, 3L, 30L, 300L);

        StatInfo stat = getPreStatMap(reporter).get(isolateKey).get(statKey);
        assertEquals(6L, stat.count.get());
        assertEquals(60L, stat.size.get());
        assertEquals(600L, stat.delay.get());
    }

    /**
     * Verify that computeIfAbsent does not overwrite the existing StatInfo instance when the
     * key is already present.
     */
    @Test
    public void testAddByKey_reuseExistingStatInfo() throws Exception {
        AuditReporterImpl reporter = new AuditReporterImpl();
        long isolateKey = 2L;
        String statKey = "key-reuse";

        invokeAddByKey(reporter, isolateKey, statKey, 1L, 1L, 1L);
        StatInfo first = getPreStatMap(reporter).get(isolateKey).get(statKey);

        invokeAddByKey(reporter, isolateKey, statKey, 5L, 50L, 500L);
        StatInfo second = getPreStatMap(reporter).get(isolateKey).get(statKey);

        assertSame("StatInfo instance must be reused, not replaced", first, second);
        assertEquals(6L, second.count.get());
        assertEquals(51L, second.size.get());
        assertEquals(501L, second.delay.get());
    }

    /**
     * Verify that different statKeys under the same isolateKey are isolated from each other.
     */
    @Test
    public void testAddByKey_isolationBetweenStatKeys() throws Exception {
        AuditReporterImpl reporter = new AuditReporterImpl();
        long isolateKey = 1L;
        String keyA = "statKey-A";
        String keyB = "statKey-B";

        invokeAddByKey(reporter, isolateKey, keyA, 1L, 10L, 100L);
        invokeAddByKey(reporter, isolateKey, keyB, 2L, 20L, 200L);

        ConcurrentHashMap<String, StatInfo> innerMap = getPreStatMap(reporter).get(isolateKey);
        assertEquals(2, innerMap.size());
        assertEquals(1L, innerMap.get(keyA).count.get());
        assertEquals(10L, innerMap.get(keyA).size.get());
        assertEquals(100L, innerMap.get(keyA).delay.get());
        assertEquals(2L, innerMap.get(keyB).count.get());
        assertEquals(20L, innerMap.get(keyB).size.get());
        assertEquals(200L, innerMap.get(keyB).delay.get());
    }

    /**
     * Verify that different isolateKeys are isolated from each other and use independent
     * inner maps.
     */
    @Test
    public void testAddByKey_isolationBetweenIsolateKeys() throws Exception {
        AuditReporterImpl reporter = new AuditReporterImpl();
        String statKey = "shared-stat-key";

        invokeAddByKey(reporter, 1L, statKey, 1L, 10L, 100L);
        invokeAddByKey(reporter, 2L, statKey, 7L, 70L, 700L);

        ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>> preStatMap = getPreStatMap(reporter);
        assertEquals(2, preStatMap.size());

        StatInfo stat1 = preStatMap.get(1L).get(statKey);
        StatInfo stat2 = preStatMap.get(2L).get(statKey);
        assertEquals(1L, stat1.count.get());
        assertEquals(10L, stat1.size.get());
        assertEquals(100L, stat1.delay.get());
        assertEquals(7L, stat2.count.get());
        assertEquals(70L, stat2.size.get());
        assertEquals(700L, stat2.delay.get());
    }

    /**
     * Verify thread safety of concurrent accumulation on the same (isolateKey, statKey),
     * i.e. after the fix there should be no NPE and no lost updates.
     */
    @Test
    public void testAddByKey_concurrentSafety() throws Exception {
        final AuditReporterImpl reporter = new AuditReporterImpl();
        final long isolateKey = 1L;
        final String statKey = "concurrent-key";

        final int threadNum = 16;
        final int loopPerThread = 5000;
        final long countPerOp = 1L;
        final long sizePerOp = 2L;
        final long delayPerOp = 3L;

        ExecutorService pool = Executors.newFixedThreadPool(threadNum);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadNum);
        final AtomicInteger errors = new AtomicInteger(0);

        for (int i = 0; i < threadNum; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int j = 0; j < loopPerThread; j++) {
                        invokeAddByKey(reporter, isolateKey, statKey, countPerOp, sizePerOp, delayPerOp);
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue("threads should finish in time", done.await(30, TimeUnit.SECONDS));
        pool.shutdownNow();

        assertEquals("no exception expected during concurrent addByKey", 0, errors.get());

        StatInfo stat = getPreStatMap(reporter).get(isolateKey).get(statKey);
        assertNotNull(stat);
        long expectedOps = (long) threadNum * loopPerThread;
        assertEquals(expectedOps * countPerOp, stat.count.get());
        assertEquals(expectedOps * sizePerOp, stat.size.get());
        assertEquals(expectedOps * delayPerOp, stat.delay.get());
    }

    /**
     * Verify that calls through the public add(...) entry point ultimately land in preStatMap.
     * This is an end-to-end integration check for addByKey.
     */
    @Test
    public void testAddByKey_viaPublicAddApi() throws Exception {
        AuditReporterImpl reporter = new AuditReporterImpl();
        long logTime = 1700000000000L;
        int auditId = 1;
        String groupId = "group1";
        String streamId = "stream1";

        reporter.add(auditId, groupId, streamId, logTime, 8L, 80L);

        ConcurrentHashMap<Long, ConcurrentHashMap<String, StatInfo>> preStatMap = getPreStatMap(reporter);
        ConcurrentHashMap<String, StatInfo> innerMap =
                preStatMap.get(AuditReporterImpl.DEFAULT_ISOLATE_KEY);
        assertNotNull("inner map under DEFAULT_ISOLATE_KEY should exist", innerMap);
        assertEquals(1, innerMap.size());

        StatInfo stat = innerMap.values().iterator().next();
        assertEquals(8L, stat.count.get());
        assertEquals(80L, stat.size.get());
    }
}
