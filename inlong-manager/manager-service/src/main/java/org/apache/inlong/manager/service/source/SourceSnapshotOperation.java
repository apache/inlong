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

package org.apache.inlong.manager.service.source;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.common.pojo.agent.TaskSnapshotMessage;
import org.apache.inlong.common.pojo.agent.TaskSnapshotRequest;
import org.apache.inlong.manager.common.enums.SourceState;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

/**
 * Operate the source snapshot
 */
@Service
public class SourceSnapshotOperation implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceSnapshotOperation.class);

    /**
     * Cache the task ip and task status, the key is task ip
     */
    private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> taskIpToIdAndStatusMap;

    public final ExecutorService executorService = new ThreadPoolExecutor(
            1,
            1,
            10L,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(100),
            new ThreadFactoryBuilder().setNameFormat("stream-source-snapshot-%s").build(),
            new CallerRunsPolicy());

    @Autowired
    private StreamSourceEntityMapper sourceMapper;

    /**
     * The queue for transfer source snapshot
     */
    private LinkedBlockingQueue<TaskSnapshotRequest> snapshotQueue = null;

    @Value("${stream.source.snapshot.batch.size:100}")
    private int batchSize = 100;

    @Value("${stream.source.snapshot.queue.size:10000}")
    private int queueSize = 10000;

    private volatile boolean isClose = false;

    /**
     * Start a thread to operate source snapshot after the app started.
     */
    @PostConstruct
    private void startSaveSnapshotTask() {
        if (snapshotQueue == null) {
            snapshotQueue = new LinkedBlockingQueue<>(queueSize);
        }
        SaveSnapshotTaskRunnable taskRunnable = new SaveSnapshotTaskRunnable();
        this.executorService.execute(taskRunnable);
        LOGGER.info("source snapshot operate thread started successfully");
    }

    /**
     * Put snapshot into data queue
     */
    public Boolean snapshot(TaskSnapshotRequest request) {
        if (request == null) {
            return true;
        }

        String agentIp = request.getAgentIp();
        List<TaskSnapshotMessage> snapshotList = request.getSnapshotList();
        if (CollectionUtils.isEmpty(snapshotList)) {
            LOGGER.info("receive snapshot from ip={}, but snapshot list is empty", agentIp);
            return true;
        }
        LOGGER.debug("receive snapshot from ip={}, msg size={}", agentIp, snapshotList.size());

        try {
            // Offer the request of snapshot to the queue, and another thread will parse the data in the queue.
            snapshotQueue.offer(request);

            // Modify the task status based on the tasks reported in the snapshot and the tasks in the cache.
            if (taskIpToIdAndStatusMap == null || taskIpToIdAndStatusMap.get(agentIp) == null) {
                LOGGER.info("success report snapshot for ip={}, task status cache is null", agentIp);
                return true;
            }
            ConcurrentHashMap<Integer, Integer> idStatusMap = taskIpToIdAndStatusMap.get(agentIp);
            Set<Integer> currentTaskIdSet = new HashSet<>();
            for (TaskSnapshotMessage snapshot : snapshotList) {
                Integer id = snapshot.getJobId();
                if (id == null) {
                    continue;
                }

                currentTaskIdSet.add(id);
                // Update the status from temporary to normal
                Integer status = idStatusMap.get(id);
                if (SourceState.TEMP_TO_NORMAL.contains(status)) {
                    sourceMapper.updateStatus(id, SourceState.SOURCE_NORMAL.getCode());
                }
            }

            // If the id in the snapshot does not contain pending deletion or pending freezing tasks,
            // then update the status to disable or frozen.
            Set<Entry<Integer, Integer>> idStatusCacheSet = idStatusMap.entrySet();
            for (Entry<Integer, Integer> entry : idStatusCacheSet) {
                Integer cacheId = entry.getKey();
                Integer cacheStatus = entry.getValue();
                if (!currentTaskIdSet.contains(cacheId)) {
                    if (Objects.equal(cacheStatus, SourceState.BEEN_ISSUED_DELETE.getCode())) {
                        sourceMapper.updateStatus(cacheId, SourceState.SOURCE_DISABLE.getCode());
                    } else if (Objects.equal(cacheStatus, SourceState.BEEN_ISSUED_FROZEN.getCode())) {
                        sourceMapper.updateStatus(cacheId, SourceState.SOURCE_FROZEN.getCode());
                    }
                }
            }

            return true;
        } catch (Throwable t) {
            LOGGER.error("put source snapshot error", t);
            return false;
        }
    }

    @Override
    public void close() {
        this.isClose = true;
    }

    /**
     * Get all tasks in a temporary state, and set up an ip-id-status map,
     * the temporary state is greater than or equal to 200.
     *
     * @see org.apache.inlong.manager.common.enums.SourceState
     */
    private ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> getTaskIpAndStatusMap() {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Integer>> ipTaskMap = new ConcurrentHashMap<>();
        List<StreamSourceEntity> sourceList = sourceMapper.selectTempStatusSource();
        for (StreamSourceEntity entity : sourceList) {
            String ip = entity.getAgentIp();
            ConcurrentHashMap<Integer, Integer> tmpMap = ipTaskMap.getOrDefault(ip, new ConcurrentHashMap<>());
            tmpMap.put(entity.getId(), entity.getStatus());
            ipTaskMap.put(ip, tmpMap);
        }

        return ipTaskMap;
    }

    /**
     * The task of saving source task snapshot into DB.
     */
    private class SaveSnapshotTaskRunnable implements Runnable {

        @Override
        public void run() {
            int cnt = 0;
            while (!isClose) {
                try {
                    TaskSnapshotRequest request = snapshotQueue.poll(1, TimeUnit.SECONDS);
                    if (request == null || CollectionUtils.isEmpty(request.getSnapshotList())) {
                        continue;
                    }

                    List<TaskSnapshotMessage> requestList = request.getSnapshotList();
                    for (TaskSnapshotMessage message : requestList) {
                        Integer id = message.getJobId();
                        StreamSourceEntity entity = new StreamSourceEntity();
                        entity.setId(id);
                        entity.setSnapshot(message.getSnapshot());
                        entity.setReportTime(request.getReportTime());

                        // update snapshot
                        sourceMapper.updateSnapshot(entity);
                    }
                    // After processing the batchSize heartbeats, get all tasks in a temporary state.
                    if (++cnt > batchSize) {
                        cnt = 0;
                        taskIpToIdAndStatusMap = getTaskIpAndStatusMap();
                    }
                } catch (Throwable t) {
                    LOGGER.error("source snapshot task runnable error", t);
                }
            }
        }
    }

}
