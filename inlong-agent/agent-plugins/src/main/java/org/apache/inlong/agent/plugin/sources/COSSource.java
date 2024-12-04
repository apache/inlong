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

package org.apache.inlong.agent.plugin.sources;

import org.apache.inlong.agent.common.AgentThreadFactory;
import org.apache.inlong.agent.conf.AgentConfiguration;
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.OffsetProfile;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.core.FileStaticManager;
import org.apache.inlong.agent.core.FileStaticManager.FileStatic;
import org.apache.inlong.agent.core.task.MemoryManager;
import org.apache.inlong.agent.core.task.OffsetManager;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.sources.extend.DefaultExtendedHandler;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;
import org.apache.inlong.agent.plugin.utils.cos.COSUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ObjectMetadata;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.TaskConstants.COS_CONTENT_STYLE;

/**
 * Read COS files
 */
public class COSSource extends AbstractSource {

    public static final int LEN_OF_FILE_OFFSET_ARRAY = 2;
    public static final String AGENT_GLOBAL_COS_SOURCE_PERMIT = "agent.global.cos.source.permit";
    public static final int DEFAULT_AGENT_GLOBAL_COS_SOURCE_PERMIT = 128 * 1000 * 1000;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected class FileOffset {

        private Long lineOffset;
        private Long byteOffset;
        private boolean hasByteOffset;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(COSSource.class);
    public static final String OFFSET_SEP = ":";
    protected final Integer WAIT_TIMEOUT_MS = 10;
    private final Integer SIZE_OF_BUFFER_TO_READ_FILE = 1024 * 1024;
    private final Long META_UPDATE_INTERVAL_MS = 10000L;
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private String fileName;
    private byte[] bufferToReadFile;
    public volatile long linePosition = 0;
    public volatile long bytePosition = 0;
    private volatile boolean fileExist = true;
    private volatile long lastInodeUpdateTime = 0;
    private COSClient cosClient;
    private String bucketName;
    private String secretId;
    private String secretKey;
    private String strRegion;
    private ObjectMetadata metadata;
    protected BlockingQueue<SourceData> queue;
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("cos-source-pool"));
    private volatile boolean running = false;

    public COSSource() {
    }

    @Override
    protected void initExtendClass() {
        extendClass = DefaultExtendedHandler.class.getCanonicalName();
    }

    @Override
    protected void initSource(InstanceProfile profile) {
        try {
            String offset = "";
            if (offsetProfile != null) {
                offset = offsetProfile.toJsonStr();
            }
            LOGGER.info("LogFileSource init: {} offset: {}", profile.toJsonStr(), offset);
            AgentConfiguration conf = AgentConfiguration.getAgentConf();
            int permit = conf.getInt(AGENT_GLOBAL_COS_SOURCE_PERMIT, DEFAULT_AGENT_GLOBAL_COS_SOURCE_PERMIT);
            MemoryManager.getInstance().addSemaphore(AGENT_GLOBAL_COS_SOURCE_PERMIT, permit);
            fileName = profile.getInstanceId();
            bucketName = profile.get(TaskConstants.COS_TASK_BUCKET_NAME);
            secretId = profile.get(TaskConstants.COS_TASK_SECRET_ID);
            secretKey = profile.get(TaskConstants.COS_TASK_SECRET_KEY);
            strRegion = profile.get(TaskConstants.COS_TASK_REGION);
            cosClient = COSUtils.createCli(secretId, secretKey, strRegion);
            metadata = cosClient.getObjectMetadata(bucketName, fileName);
            queue = new LinkedBlockingQueue<>(CACHE_QUEUE_SIZE);
            bufferToReadFile = new byte[SIZE_OF_BUFFER_TO_READ_FILE];
            lastInodeUpdateTime = AgentUtils.getCurrentTime();
            initOffset(taskId);
            EXECUTOR_SERVICE.execute(run());
        } catch (Exception ex) {
            stopRunning();
            throw new FileException("error init stream for " + fileName, ex);
        }
    }

    @Override
    protected boolean doPrepareToRead() {
        if (AgentUtils.getCurrentTime() - lastInodeUpdateTime > META_UPDATE_INTERVAL_MS) {
            metadata = cosClient.getObjectMetadata(bucketName, fileName);
            lastInodeUpdateTime = AgentUtils.getCurrentTime();
        }
        if (metadata.getContentLength() < bytePosition) {
            fileExist = false;
            LOGGER.info("file rotate, instance will restart and offset will be clean, file {}",
                    fileName);
            return false;
        }
        return true;
    }

    @Override
    protected List<SourceData> readFromSource() {
        if (queue.isEmpty()) {
            return null;
        }
        int count = 0;
        int len = 0;
        List<SourceData> lines = new ArrayList<>();
        while (!queue.isEmpty() && count < BATCH_READ_LINE_COUNT && len < BATCH_READ_LINE_TOTAL_LEN) {
            if (len + queue.peek().getData().length > BATCH_READ_LINE_TOTAL_LEN) {
                break;
            }
            len += queue.peek().getData().length;
            count++;
            lines.add(queue.poll());
        }
        MemoryManager.getInstance().release(AGENT_GLOBAL_COS_SOURCE_PERMIT, len);
        return lines;
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("path is {}, linePosition {}, bytePosition is {} file len {}", fileName, linePosition,
                bytePosition, metadata.getContentLength());
    }

    @Override
    protected String getThreadName() {
        return "cos-file-source-" + taskId + "-" + fileName;
    }

    private void initOffset(String taskId) {
        long lineOffset;
        long byteOffset;
        if (offsetProfile != null) {
            FileOffset fileOffset = parseFIleOffset(offsetProfile.getOffset());
            lineOffset = fileOffset.lineOffset;
            byteOffset = fileOffset.byteOffset;
            LOGGER.info("initOffset inode no change taskId {} restore lineOffset {} byteOffset {}, file {}", taskId,
                    lineOffset, byteOffset, fileName);
        } else {
            lineOffset = 0;
            byteOffset = 0;
            LOGGER.info("initOffset taskId {} for new all read lineOffset {} byteOffset {} file {}", taskId,
                    lineOffset, byteOffset, fileName);
        }
        linePosition = lineOffset;
        bytePosition = byteOffset;
    }

    private Runnable run() {
        return () -> {
            AgentThreadFactory.nameThread(getThreadName());
            running = true;
            try {
                doRun();
            } catch (Throwable e) {
                LOGGER.error("do run error maybe file deleted: ", e);
                ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
            }
            running = false;
        };
    }

    /**
     * Read new lines.
     *
     * @return The new position after the lines have been read
     * @throws IOException if an I/O error occurs.
     */
    protected void doRun() throws IOException {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, fileName);
        getObjectRequest.setRange(bytePosition, metadata.getContentLength());
        COSObject cosObject = cosClient.getObject(getObjectRequest);
        InputStream inputStream = cosObject.getObjectContent();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int num;
        boolean overLen = false;
        while ((num = inputStream.read(bufferToReadFile)) != -1) {
            LOGGER.debug("read size {}", num);
            for (int i = 0; i < num; i++) {
                byte ch = bufferToReadFile[i];
                bytePosition++;
                switch (ch) {
                    case '\n':
                        linePosition++;
                        boolean suc = false;
                        while (isRunnable() && !suc) {
                            SourceData sourceData = new SourceData(baos.toByteArray(),
                                    getOffsetString(linePosition, bytePosition));
                            boolean suc4Queue = waitForPermit(AGENT_GLOBAL_COS_SOURCE_PERMIT,
                                    sourceData.getData().length);
                            if (!suc4Queue) {
                                break;
                            }
                            suc = queue.offer(sourceData);
                            if (!suc) {
                                MemoryManager.getInstance()
                                        .release(AGENT_GLOBAL_COS_SOURCE_PERMIT, sourceData.getData().length);
                                AgentUtils.silenceSleepInMs(WAIT_TIMEOUT_MS);
                            }
                        }
                        if (overLen) {
                            LOGGER.warn("readLines over len finally string len {}",
                                    new String(baos.toByteArray()).length());
                            long auditTime = 0;
                            auditTime = profile.getSinkDataTime();
                            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_FAILED, inlongGroupId, inlongStreamId,
                                    auditTime, 1, maxPackSize, auditVersion);
                            AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_FAILED_REAL_TIME, inlongGroupId,
                                    inlongStreamId, AgentUtils.getCurrentTime(), 1, maxPackSize, auditVersion);
                        }
                        baos.reset();
                        overLen = false;
                        break;
                    case '\r':
                        break;
                    default:
                        if (baos.size() < maxPackSize) {
                            baos.write(ch);
                        } else {
                            overLen = true;
                        }
                }
            }
        }
        baos.close();
        inputStream.close();
        cosObject.close();
    }

    private String getOffsetString(Long lineOffset, Long byteOffset) {
        return lineOffset + OFFSET_SEP + byteOffset;
    }

    private FileOffset parseFIleOffset(String offset) {
        String[] offsetArray = offset.split(OFFSET_SEP);
        if (offsetArray.length == LEN_OF_FILE_OFFSET_ARRAY) {
            return new FileOffset(Long.parseLong(offsetArray[0]), Long.parseLong(offsetArray[1]), true);
        } else {
            return new FileOffset(Long.parseLong(offsetArray[0]), null, false);
        }
    }

    @Override
    protected boolean isRunnable() {
        return runnable && fileExist;
    }

    @Override
    public boolean sourceExist() {
        return fileExist;
    }

    @Override
    protected void releaseSource() {
        while (running) {
            AgentUtils.silenceSleepInMs(1);
        }
        if (cosClient != null) {
            FileStatic data = new FileStatic();
            data.setTaskId(taskId);
            data.setRetry(String.valueOf(profile.isRetry()));
            data.setContentType(profile.get(COS_CONTENT_STYLE));
            data.setGroupId(profile.getInlongGroupId());
            data.setStreamId(profile.getInlongStreamId());
            data.setDataTime(format.format(profile.getSinkDataTime()));
            data.setFileName(profile.getInstanceId());
            data.setFileLen(String.valueOf(metadata.getContentLength()));
            data.setReadBytes(String.valueOf(bytePosition));
            data.setReadLines(String.valueOf(linePosition));
            OffsetProfile offsetProfile = OffsetManager.getInstance().getOffset(taskId, instanceId);
            if (offsetProfile != null) {
                data.setSendLines(offsetProfile.getOffset());
            }
            FileStaticManager.putStaticMsg(data);
            cosClient.shutdown();
        }
        while (!queue.isEmpty()) {
            MemoryManager.getInstance().release(AGENT_GLOBAL_COS_SOURCE_PERMIT, queue.poll().getData().length);
        }
    }

    private boolean waitForPermit(String permitName, int permitLen) {
        boolean suc = false;
        while (!suc) {
            suc = MemoryManager.getInstance().tryAcquire(permitName, permitLen);
            if (!suc) {
                MemoryManager.getInstance().printDetail(permitName, "cos_source");
                if (!isRunnable()) {
                    return false;
                }
                AgentUtils.silenceSleepInMs(WAIT_TIMEOUT_MS);
            }
        }
        return true;
    }
}
