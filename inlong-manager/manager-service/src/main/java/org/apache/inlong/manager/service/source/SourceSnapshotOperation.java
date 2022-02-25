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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.common.pojo.agent.TaskSnapshotMessage;
import org.apache.inlong.common.pojo.agent.TaskSnapshotRequest;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.dao.mapper.StreamSourceEntityMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
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

    // The queue for transfer source snapshot
    private LinkedBlockingQueue<TaskSnapshotRequest> snapshotQueue = null;

    @Value("${stream.source.snapshot.batch.size:100}")
    private int batchSize;

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
    public Boolean putData(TaskSnapshotRequest request) {
        if (request == null || CollectionUtils.isEmpty(request.getSnapshotList())) {
            LOGGER.info("request received, but snapshot list is empty, just return");
            return true;
        }
        try {
            snapshotQueue.offer(request);
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
     * The task of saving source task snapshot into DB.
     */
    private class SaveSnapshotTaskRunnable implements Runnable {

        @Override
        public void run() {
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
                } catch (Throwable t) {
                    LOGGER.error("source snapshot task runnable error", t);
                }
            }
        }
    }

}
