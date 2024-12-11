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

package org.apache.inlong.agent.core.task;

import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.utils.AgentUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_QUEUE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_SOURCE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_WRITER_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_GLOBAL_READER_QUEUE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_GLOBAL_READER_SOURCE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_GLOBAL_WRITER_PERMIT;

/**
 * used to limit global memory to avoid oom
 */
public class MemoryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryManager.class);
    private static volatile MemoryManager memoryManager = null;
    private final AgentConfiguration conf;
    private ConcurrentHashMap<String, Semaphore> semaphoreMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Long> lastPrintTime = new ConcurrentHashMap<>();
    private static final int PRINT_INTERVAL_MS = 1000;
    private Set<String> defaultSemaphoreTypes = new HashSet<>();

    private MemoryManager() {
        this.conf = AgentConfiguration.getAgentConf();
        Semaphore semaphore = null;
        semaphore = new Semaphore(
                conf.getInt(AGENT_GLOBAL_READER_SOURCE_PERMIT, DEFAULT_AGENT_GLOBAL_READER_SOURCE_PERMIT));
        semaphoreMap.put(AGENT_GLOBAL_READER_SOURCE_PERMIT, semaphore);
        lastPrintTime.put(AGENT_GLOBAL_READER_SOURCE_PERMIT, 0L);
        defaultSemaphoreTypes.add(AGENT_GLOBAL_READER_SOURCE_PERMIT);

        semaphore = new Semaphore(
                conf.getInt(AGENT_GLOBAL_READER_QUEUE_PERMIT, DEFAULT_AGENT_GLOBAL_READER_QUEUE_PERMIT));
        semaphoreMap.put(AGENT_GLOBAL_READER_QUEUE_PERMIT, semaphore);
        lastPrintTime.put(AGENT_GLOBAL_READER_QUEUE_PERMIT, 0L);
        defaultSemaphoreTypes.add(AGENT_GLOBAL_READER_QUEUE_PERMIT);

        semaphore = new Semaphore(
                conf.getInt(AGENT_GLOBAL_WRITER_PERMIT, DEFAULT_AGENT_GLOBAL_WRITER_PERMIT));
        semaphoreMap.put(AGENT_GLOBAL_WRITER_PERMIT, semaphore);
        lastPrintTime.put(AGENT_GLOBAL_WRITER_PERMIT, 0L);
        defaultSemaphoreTypes.add(AGENT_GLOBAL_WRITER_PERMIT);
    }

    /**
     * manager singleton
     */
    public static MemoryManager getInstance() {
        if (memoryManager == null) {
            synchronized (MemoryManager.class) {
                if (memoryManager == null) {
                    memoryManager = new MemoryManager();
                }
            }
        }
        return memoryManager;
    }

    public void addSemaphore(String semaphoreType, int permit) {
        if (semaphoreMap.containsKey(semaphoreType)) {
            return;
        }
        synchronized (MemoryManager.class) {
            if (semaphoreMap.containsKey(semaphoreType)) {
                return;
            }
            Semaphore semaphore = new Semaphore(permit);
            semaphoreMap.put(semaphoreType, semaphore);
            lastPrintTime.put(semaphoreType, 0L);
        }
    }

    public boolean tryAcquire(String semaphoreName, int permit) {
        Semaphore semaphore = semaphoreMap.get(semaphoreName);
        if (semaphore == null) {
            LOGGER.error("tryAcquire {} not exist");
            return false;
        }
        return semaphore.tryAcquire(permit);
    }

    public void release(String semaphoreName, int permit) {
        Semaphore semaphore = semaphoreMap.get(semaphoreName);
        if (semaphore == null) {
            LOGGER.error("release {} not exist");
            return;
        }
        semaphore.release(permit);
    }

    public int getLeft(String semaphoreName) {
        Semaphore semaphore = semaphoreMap.get(semaphoreName);
        if (semaphore == null) {
            LOGGER.error("getLeft {} not exist");
            return -1;
        }
        return semaphore.availablePermits();
    }

    public void printDetail(String semaphoreName, String detail) {
        Semaphore semaphore = semaphoreMap.get(semaphoreName);
        if (semaphore == null) {
            LOGGER.error("printDetail {} not exist", semaphoreName);
            return;
        }
        if (AgentUtils.getCurrentTime() - lastPrintTime.get(semaphoreName) > PRINT_INTERVAL_MS) {
            LOGGER.info("{} permit left {} wait {} {}", detail, semaphore.availablePermits(),
                    semaphore.getQueueLength(),
                    semaphoreName);
            lastPrintTime.put(semaphoreName, AgentUtils.getCurrentTime());
        }
    }

    public void printAll() {
        printDetail(AGENT_GLOBAL_READER_SOURCE_PERMIT, "printAll");
        printDetail(AGENT_GLOBAL_READER_QUEUE_PERMIT, "printAll");
        printDetail(AGENT_GLOBAL_WRITER_PERMIT, "printAll");
        semaphoreMap.entrySet().forEach(entry -> {
            if (!defaultSemaphoreTypes.contains(entry.getKey())) {
                printDetail(entry.getKey(), "printAll");
            }
        });
    }
}
