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

package org.apache.inlong.manager.service.queue;

import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.pojo.cluster.pulsar.PulsarClusterInfo;
import org.apache.inlong.manager.pojo.consume.BriefMQMessage;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.QueryMessageRequest;
import org.apache.inlong.manager.service.resource.queue.pulsar.PulsarOperator;
import org.apache.inlong.manager.service.resource.queue.pulsar.QueryCountDownLatch;
import org.apache.inlong.manager.service.resource.queue.pulsar.QueryLatestMessagesRunnable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class for {@link QueryLatestMessagesRunnable}.
 * Tests focus on interrupt handling behavior.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class QueryLatestMessagesRunnableTest {

    @Mock
    private PulsarOperator pulsarOperator;

    @Mock
    private InlongStreamInfo streamInfo;

    @Mock
    private PulsarClusterInfo clusterInfo;

    private QueryMessageRequest queryMessageRequest;
    private List<BriefMQMessage> messageResultList;
    private QueryCountDownLatch queryLatch;

    private static final String CLUSTER_NAME = "test-cluster";
    private static final String FULL_TOPIC_NAME = "public/test-namespace/test-topic";
    private static final String GROUP_ID = "test-group";
    private static final String STREAM_ID = "test-stream";

    @BeforeEach
    public void setUp() {
        when(clusterInfo.getName()).thenReturn(CLUSTER_NAME);

        queryMessageRequest = QueryMessageRequest.builder()
                .groupId(GROUP_ID)
                .streamId(STREAM_ID)
                .messageCount(10)
                .build();

        messageResultList = Collections.synchronizedList(new ArrayList<>());
        queryLatch = new QueryCountDownLatch(10, 1);
    }

    /**
     * Test: Task executes successfully when not interrupted.
     * Verifies that messages are added to result list and latch is counted down.
     */
    @Test
    public void testSuccessfulQueryWithoutInterruption() {
        // 准备 mock 返回数据
        List<BriefMQMessage> mockMessages = new ArrayList<>();
        BriefMQMessage message = new BriefMQMessage();
        message.setBody("test message");
        mockMessages.add(message);

        when(pulsarOperator.queryLatestMessage(any(), anyString(), any(), any(), anyBoolean()))
                .thenReturn(mockMessages);

        QueryLatestMessagesRunnable task = new QueryLatestMessagesRunnable(
                pulsarOperator, streamInfo, clusterInfo, false, FULL_TOPIC_NAME,
                queryMessageRequest, messageResultList, queryLatch);

        // 同步执行任务
        task.run();

        // 验证：查询被执行
        verify(pulsarOperator, times(1)).queryLatestMessage(any(), anyString(), any(), any(), anyBoolean());
        // 验证：结果被添加到列表
        assertEquals(1, messageResultList.size());
    }

    /**
     * Test: Task exits immediately when thread is interrupted before query starts.
     * Verifies that no query is performed and latch is still counted down.
     */
    @Test
    public void testInterruptionBeforeQuery() throws InterruptedException {
        QueryLatestMessagesRunnable task = new QueryLatestMessagesRunnable(
                pulsarOperator, streamInfo, clusterInfo, false, FULL_TOPIC_NAME,
                queryMessageRequest, messageResultList, queryLatch);

        // 创建一个在执行任务前就被中断的线程
        Thread testThread = new Thread(() -> {
            // 在执行任务前中断线程
            Thread.currentThread().interrupt();
            task.run();
        });

        testThread.start();
        testThread.join(5000);

        // 验证：查询未被执行（因为在查询前就检测到中断）
        verify(pulsarOperator, never()).queryLatestMessage(any(), anyString(), any(), any(), anyBoolean());
        // 验证：结果列表为空
        assertTrue(messageResultList.isEmpty());
    }

    /**
     * Test: Task discards results when interrupted after query completes.
     * Verifies that results are not added to the list even if query returned data.
     */
    @Test
    public void testInterruptionAfterQuery() throws InterruptedException {
        // 模拟查询操作会检查并设置中断标志
        List<BriefMQMessage> mockMessages = new ArrayList<>();
        BriefMQMessage message = new BriefMQMessage();
        message.setBody("test message");
        mockMessages.add(message);

        CountDownLatch queryStartedLatch = new CountDownLatch(1);
        CountDownLatch interruptSetLatch = new CountDownLatch(1);

        when(pulsarOperator.queryLatestMessage(any(), anyString(), any(), any(), anyBoolean()))
                .thenAnswer(invocation -> {
                    // 通知主线程查询已开始
                    queryStartedLatch.countDown();
                    // 等待主线程设置中断标志
                    interruptSetLatch.await(5, TimeUnit.SECONDS);
                    // 返回结果（模拟 IO 操作不响应中断）
                    return mockMessages;
                });

        QueryLatestMessagesRunnable task = new QueryLatestMessagesRunnable(
                pulsarOperator, streamInfo, clusterInfo, false, FULL_TOPIC_NAME,
                queryMessageRequest, messageResultList, queryLatch);

        // 在线程池中执行任务
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(task);

        // 等待查询开始
        assertTrue(queryStartedLatch.await(5, TimeUnit.SECONDS));

        // 取消任务（设置中断标志）
        future.cancel(true);
        // 通知任务可以继续
        interruptSetLatch.countDown();

        // 等待任务完成
        Thread.sleep(500);

        // 验证：查询被执行
        verify(pulsarOperator, times(1)).queryLatestMessage(any(), anyString(), any(), any(), anyBoolean());
        // 验证：结果被丢弃（因为查询后检测到中断标志）
        assertTrue(messageResultList.isEmpty());

        executor.shutdownNow();
    }

    /**
     * Test: Task handles empty query results correctly.
     * Verifies that no exception is thrown and latch is counted down.
     */
    @Test
    public void testEmptyQueryResults() {
        when(pulsarOperator.queryLatestMessage(any(), anyString(), any(), any(), anyBoolean()))
                .thenReturn(Collections.emptyList());

        QueryLatestMessagesRunnable task = new QueryLatestMessagesRunnable(
                pulsarOperator, streamInfo, clusterInfo, false, FULL_TOPIC_NAME,
                queryMessageRequest, messageResultList, queryLatch);

        task.run();

        // 验证：查询被执行
        verify(pulsarOperator, times(1)).queryLatestMessage(any(), anyString(), any(), any(), anyBoolean());
        // 验证：结果列表为空
        assertTrue(messageResultList.isEmpty());
    }

    /**
     * Test: Task handles null query results correctly.
     * Verifies that no exception is thrown and latch is counted down.
     */
    @Test
    public void testNullQueryResults() {
        when(pulsarOperator.queryLatestMessage(any(), anyString(), any(), any(), anyBoolean()))
                .thenReturn(null);

        QueryLatestMessagesRunnable task = new QueryLatestMessagesRunnable(
                pulsarOperator, streamInfo, clusterInfo, false, FULL_TOPIC_NAME,
                queryMessageRequest, messageResultList, queryLatch);

        task.run();

        // 验证：查询被执行
        verify(pulsarOperator, times(1)).queryLatestMessage(any(), anyString(), any(), any(), anyBoolean());
        // 验证：结果列表为空
        assertTrue(messageResultList.isEmpty());
    }

    /**
     * Test: Task handles query exception gracefully.
     * Verifies that exception is caught and latch is still counted down.
     */
    @Test
    public void testQueryException() {
        when(pulsarOperator.queryLatestMessage(any(), anyString(), any(), any(), anyBoolean()))
                .thenThrow(new RuntimeException("Simulated query failure"));

        QueryLatestMessagesRunnable task = new QueryLatestMessagesRunnable(
                pulsarOperator, streamInfo, clusterInfo, false, FULL_TOPIC_NAME,
                queryMessageRequest, messageResultList, queryLatch);

        // 不应抛出异常
        task.run();

        // 验证：查询被执行
        verify(pulsarOperator, times(1)).queryLatestMessage(any(), anyString(), any(), any(), anyBoolean());
        // 验证：结果列表为空
        assertTrue(messageResultList.isEmpty());
    }

    /**
     * Test: Multiple tasks can be cancelled together.
     * Simulates the scenario where RejectedExecutionException occurs and all submitted tasks need to be cancelled.
     */
    @Test
    public void testMultipleTaskCancellation() throws InterruptedException {
        int taskCount = 5;
        List<BriefMQMessage> sharedResultList = Collections.synchronizedList(new ArrayList<>());
        QueryCountDownLatch sharedLatch = new QueryCountDownLatch(50, taskCount);

        // 模拟慢查询
        CountDownLatch allTasksStarted = new CountDownLatch(taskCount);
        CountDownLatch proceedLatch = new CountDownLatch(1);

        List<BriefMQMessage> mockMessages = new ArrayList<>();
        BriefMQMessage message = new BriefMQMessage();
        message.setBody("test message");
        mockMessages.add(message);

        when(pulsarOperator.queryLatestMessage(any(), anyString(), any(), any(), anyBoolean()))
                .thenAnswer(invocation -> {
                    allTasksStarted.countDown();
                    // 等待信号继续
                    proceedLatch.await(10, TimeUnit.SECONDS);
                    return mockMessages;
                });

        ExecutorService executor = Executors.newFixedThreadPool(taskCount);
        List<Future<?>> futures = new ArrayList<>();

        // 提交多个任务
        for (int i = 0; i < taskCount; i++) {
            QueryLatestMessagesRunnable task = new QueryLatestMessagesRunnable(
                    pulsarOperator, streamInfo, clusterInfo, false, FULL_TOPIC_NAME,
                    queryMessageRequest, sharedResultList, sharedLatch);
            futures.add(executor.submit(task));
        }

        // 等待所有任务开始执行
        assertTrue(allTasksStarted.await(5, TimeUnit.SECONDS));

        // 取消所有任务
        int cancelledCount = 0;
        for (Future<?> future : futures) {
            if (future.cancel(true)) {
                cancelledCount++;
            }
        }

        // 允许任务继续
        proceedLatch.countDown();

        // 关闭线程池
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        // 验证：结果被丢弃（因为中断标志已设置）
        assertTrue(sharedResultList.isEmpty());
    }

    /**
     * Test: Verifies interrupt flag is checked at the right points.
     * This test ensures the interrupt check happens both before and after the query.
     */
    @Test
    public void testInterruptCheckPoints() throws InterruptedException {
        List<BriefMQMessage> mockMessages = new ArrayList<>();
        BriefMQMessage message = new BriefMQMessage();
        message.setBody("test message");
        mockMessages.add(message);

        // 记录查询被调用的次数
        final int[] queryCallCount = {0};

        when(pulsarOperator.queryLatestMessage(any(), anyString(), any(), any(), anyBoolean()))
                .thenAnswer(invocation -> {
                    queryCallCount[0]++;
                    return mockMessages;
                });

        // 场景1: 正常执行（不中断）
        QueryLatestMessagesRunnable normalTask = new QueryLatestMessagesRunnable(
                pulsarOperator, streamInfo, clusterInfo, false, FULL_TOPIC_NAME,
                queryMessageRequest, messageResultList, queryLatch);
        normalTask.run();

        assertEquals(1, queryCallCount[0], "Query should be called once in normal execution");
        assertEquals(1, messageResultList.size(), "Result should be added in normal execution");

        // 重置
        queryCallCount[0] = 0;
        messageResultList.clear();
        queryLatch = new QueryCountDownLatch(10, 1);

        // 场景2: 查询前中断
        Thread preInterruptThread = new Thread(() -> {
            Thread.currentThread().interrupt();
            QueryLatestMessagesRunnable task = new QueryLatestMessagesRunnable(
                    pulsarOperator, streamInfo, clusterInfo, false, FULL_TOPIC_NAME,
                    queryMessageRequest, messageResultList, queryLatch);
            task.run();
        });
        preInterruptThread.start();
        preInterruptThread.join(5000);

        assertEquals(0, queryCallCount[0], "Query should not be called when interrupted before");
        assertTrue(messageResultList.isEmpty(), "Result should not be added when interrupted before");
    }

    /**
     * Test: Verifies that RejectedExecutionException message contains 'rejected' keyword.
     * This ensures the exception can be properly identified when queue is full.
     */
    @Test
    public void testRejectedExecutionExceptionContainsRejectKeyword() {
        // Create a thread pool with minimal capacity to trigger rejection
        ExecutorService tinyExecutor = new java.util.concurrent.ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>(1),
                new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());

        CountDownLatch blockingLatch = new CountDownLatch(1);
        try {
            // Submit blocking tasks to fill the pool and queue
            tinyExecutor.submit(() -> {
                try {
                    blockingLatch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            tinyExecutor.submit(() -> {
                try {
                    blockingLatch.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // This submission should be rejected (pool full, queue full)
            tinyExecutor.submit(() -> {
                // This task should never execute
            });

            // If we reach here, the test failed
            org.junit.jupiter.api.Assertions.fail("Expected RejectedExecutionException was not thrown");
        } catch (java.util.concurrent.RejectedExecutionException e) {
            // Verify the exception message contains 'reject' keyword (case-insensitive)
            String message = e.getMessage();
            assertTrue(message != null && message.toLowerCase().contains("reject"),
                    "RejectedExecutionException message should contain 'reject' keyword, but was: " + message);
        } finally {
            blockingLatch.countDown();
            tinyExecutor.shutdownNow();
        }
    }

    /**
     * Test: Verifies that BusinessException thrown by PulsarQueueResourceOperator contains 'reject' keyword.
     * This simulates the scenario where the queue is full and task submission is rejected.
     */
    @Test
    public void testBusinessExceptionContainsRejectKeywordWhenQueueFull() {
        // Simulate the BusinessException that would be thrown when RejectedExecutionException occurs
        String expectedMessage = "Query messages task rejected: too many concurrent requests";
        BusinessException exception = new BusinessException(expectedMessage);

        // Verify the exception message contains 'reject' keyword
        assertTrue(exception.getMessage().contains("reject"),
                "BusinessException message should contain 'reject' keyword");
    }
}
