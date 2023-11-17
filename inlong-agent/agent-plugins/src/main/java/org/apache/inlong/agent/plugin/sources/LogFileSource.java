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
import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.OffsetProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.DataCollectType;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.core.task.OffsetManager;
import org.apache.inlong.agent.core.task.file.MemoryManager;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.file.Reader;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;
import org.apache.inlong.agent.plugin.sources.reader.file.KubernetesMetadataProvider;
import org.apache.inlong.agent.plugin.utils.file.FileDataUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.DateTransUtils;

import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.CommonConstants.COMMA;
import static org.apache.inlong.agent.constant.CommonConstants.DEFAULT_PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_KEY_DATA;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_PACKAGE_MAX_SIZE;
import static org.apache.inlong.agent.constant.CommonConstants.PROXY_SEND_PARTITION_KEY;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_QUEUE_PERMIT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_GLOBAL_READER_SOURCE_PERMIT;
import static org.apache.inlong.agent.constant.KubernetesConstants.KUBERNETES;
import static org.apache.inlong.agent.constant.MetadataConstants.DATA_CONTENT;
import static org.apache.inlong.agent.constant.MetadataConstants.DATA_CONTENT_TIME;
import static org.apache.inlong.agent.constant.MetadataConstants.ENV_CVM;
import static org.apache.inlong.agent.constant.MetadataConstants.METADATA_FILE_NAME;
import static org.apache.inlong.agent.constant.MetadataConstants.METADATA_HOST_NAME;
import static org.apache.inlong.agent.constant.MetadataConstants.METADATA_SOURCE_IP;
import static org.apache.inlong.agent.constant.TaskConstants.JOB_FILE_META_ENV_LIST;
import static org.apache.inlong.agent.constant.TaskConstants.OFFSET;
import static org.apache.inlong.agent.constant.TaskConstants.TASK_CYCLE_UNIT;

/**
 * Read text files
 */
public class LogFileSource extends AbstractSource {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private class SourceData {

        private String data;
        private Long offset;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(LogFileSource.class);
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("log-file-source"));
    private final Integer BATCH_READ_LINE_COUNT = 10000;
    private final Integer BATCH_READ_LINE_TOTAL_LEN = 1024 * 1024;
    private final Integer CORE_THREAD_PRINT_INTERVAL_MS = 1000;
    private final Integer CACHE_QUEUE_SIZE = 10 * BATCH_READ_LINE_COUNT;
    private final Integer SIZE_OF_BUFFER_TO_READ_FILE = 64 * 1024;
    private final Integer EMPTY_CHECK_COUNT_AT_LEAST = 30;
    private final Long INODE_UPDATE_INTERVAL_MS = 1000L;
    private final Integer READ_WAIT_TIMEOUT_MS = 10;
    private final SimpleDateFormat RECORD_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public InstanceProfile profile;
    private String taskId;
    private String instanceId;
    private int maxPackSize;
    private String fileName;
    private File file;
    private byte[] bufferToReadFile;
    public volatile long linePosition = 0;
    public volatile long bytePosition = 0;
    private boolean needMetadata = false;
    public Map<String, String> metadata;
    private boolean isIncrement = false;
    private BlockingQueue<SourceData> queue;
    private final Gson GSON = new Gson();
    private volatile boolean runnable = true;
    private volatile boolean fileExist = true;
    private String inodeInfo;
    private volatile long lastInodeUpdateTime = 0;
    private volatile boolean running = false;
    private long dataTime = 0;
    private volatile long emptyCount = 0;

    public LogFileSource() {
        OffsetManager.init();
    }

    @Override
    public void init(InstanceProfile profile) {
        try {
            LOGGER.info("LogFileSource init: {}", profile.toJsonStr());
            this.profile = profile;
            super.init(profile);
            taskId = profile.getTaskId();
            instanceId = profile.getInstanceId();
            fileName = profile.getInstanceId();
            maxPackSize = profile.getInt(PROXY_PACKAGE_MAX_SIZE, DEFAULT_PROXY_PACKAGE_MAX_SIZE);
            bufferToReadFile = new byte[SIZE_OF_BUFFER_TO_READ_FILE];
            isIncrement = isIncrement(profile);
            file = new File(fileName);
            inodeInfo = profile.get(TaskConstants.INODE_INFO);
            lastInodeUpdateTime = AgentUtils.getCurrentTime();
            linePosition = getInitLineOffset(isIncrement, taskId, instanceId, inodeInfo);
            bytePosition = getBytePositionByLine(linePosition);
            queue = new LinkedBlockingQueue<>(CACHE_QUEUE_SIZE);
            dataTime = DateTransUtils.timeStrConvertTomillSec(profile.getSourceDataTime(),
                    profile.get(TASK_CYCLE_UNIT));
            try {
                registerMeta(profile);
            } catch (Exception ex) {
                LOGGER.error("init metadata error", ex);
            }
            EXECUTOR_SERVICE.execute(coreThread());
        } catch (Exception ex) {
            stopRunning();
            throw new FileException("error init stream for " + file.getPath(), ex);
        }
    }

    private int getRealLineCount(String fileName) {
        try (LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(instanceId))) {
            lineNumberReader.skip(Long.MAX_VALUE);
            return lineNumberReader.getLineNumber();
        } catch (IOException ex) {
            LOGGER.error("getRealLineCount error {} file {}", ex.getMessage(), fileName);
            return 0;
        }
    }

    private long getInitLineOffset(boolean isIncrement, String taskId, String instanceId, String inodeInfo) {
        OffsetProfile offsetProfile = OffsetManager.getInstance().getOffset(taskId, instanceId);
        int fileLineCount = getRealLineCount(instanceId);
        long offset = 0;
        if (offsetProfile != null && offsetProfile.getInodeInfo().compareTo(inodeInfo) == 0) {
            offset = offsetProfile.getOffset();
            if (fileLineCount < offset) {
                LOGGER.info("getInitLineOffset inode no change taskId {} file rotate, offset set to 0, file {}", taskId,
                        fileName);
                offset = 0;
            } else {
                LOGGER.info("getInitLineOffset inode no change taskId {} from db {}, file {}", taskId, offset,
                        fileName);
            }
        } else {
            if (isIncrement) {
                offset = getRealLineCount(instanceId);
                LOGGER.info("getInitLineOffset taskId {} for new increment read from {} file {}", taskId,
                        offset, fileName);
            } else {
                offset = 0;
                LOGGER.info("getInitLineOffset taskId {} for new all read from 0 file {}", taskId, fileName);
            }
        }
        return offset;
    }

    public File getFile() {
        return file;
    }

    public void registerMeta(InstanceProfile jobConf) {
        if (!jobConf.hasKey(JOB_FILE_META_ENV_LIST)) {
            return;
        }
        String[] env = jobConf.get(JOB_FILE_META_ENV_LIST).split(COMMA);
        Arrays.stream(env).forEach(data -> {
            if (data.equalsIgnoreCase(KUBERNETES)) {
                needMetadata = true;
                new KubernetesMetadataProvider(this).getData();
            } else if (data.equalsIgnoreCase(ENV_CVM)) {
                needMetadata = true;
                metadata.put(METADATA_HOST_NAME, AgentUtils.getLocalHost());
                metadata.put(METADATA_SOURCE_IP, AgentUtils.fetchLocalIp());
                metadata.put(METADATA_FILE_NAME, file.getName());
            }
        });
    }

    private boolean isIncrement(InstanceProfile profile) {
        if (profile.hasKey(TaskConstants.JOB_FILE_CONTENT_COLLECT_TYPE) && DataCollectType.INCREMENT
                .equalsIgnoreCase(profile.get(TaskConstants.JOB_FILE_CONTENT_COLLECT_TYPE))) {
            return true;
        }
        return false;
    }

    private long getBytePositionByLine(long linePosition) throws IOException {
        long pos = 0;
        long readCount = 0;
        RandomAccessFile input = null;
        try {
            input = new RandomAccessFile(file, "r");
            while (readCount < linePosition) {
                List<String> lines = new ArrayList<>();
                pos = readLines(input, pos, lines, Math.min((int) (linePosition - readCount), BATCH_READ_LINE_COUNT),
                        BATCH_READ_LINE_TOTAL_LEN, true);
                readCount += lines.size();
                if (lines.size() == 0) {
                    LOGGER.error("getBytePositionByLine LineNum {} larger than the real file");
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.error("getBytePositionByLine error: ", e);
        } finally {
            if (input != null) {
                input.close();
            }
        }
        LOGGER.info("getBytePositionByLine {} LineNum {} position {}", fileName, linePosition, pos);
        return pos;
    }

    /**
     * Read new lines.
     *
     * @param reader The file to read
     * @return The new position after the lines have been read
     * @throws IOException if an I/O error occurs.
     */
    private long readLines(RandomAccessFile reader, long pos, List<String> lines, int maxLineCount, int maxLineTotalLen,
            boolean isCounting)
            throws IOException {
        if (maxLineCount == 0) {
            return pos;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        reader.seek(pos);
        long rePos = pos; // position to re-read
        int num;
        int lineTotalLen = 0;
        boolean overLen = false;
        while ((num = reader.read(bufferToReadFile)) != -1) {
            LOGGER.debug("read size {}", num);
            int i = 0;
            for (; i < num; i++) {
                byte ch = bufferToReadFile[i];
                switch (ch) {
                    case '\n':
                        if (isCounting) {
                            lines.add(new String(""));
                        } else {
                            String temp = new String(baos.toByteArray(), StandardCharsets.UTF_8);
                            lines.add(temp);
                            lineTotalLen += temp.length();
                        }
                        rePos = pos + i + 1;
                        if (overLen) {
                            LOGGER.warn("readLines over len finally string len {}",
                                    new String(baos.toByteArray()).length());
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
                if (lines.size() >= maxLineCount || lineTotalLen >= maxLineTotalLen) {
                    break;
                }
            }
            if (lines.size() >= maxLineCount || lineTotalLen >= maxLineTotalLen) {
                break;
            }
            if (i == num) {
                pos = reader.getFilePointer();
            }
        }
        baos.close();
        reader.seek(rePos); // Ensure we can re-read if necessary
        return rePos;
    }

    @Override
    public Message read() {
        SourceData sourceData = null;
        try {
            sourceData = queue.poll(READ_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("poll {} data get interrupted.", file.getPath(), e);
        }
        if (sourceData == null) {
            return null;
        }
        MemoryManager.getInstance().release(AGENT_GLOBAL_READER_QUEUE_PERMIT, sourceData.data.length());
        Message finalMsg = createMessage(sourceData);
        return finalMsg;
    }

    private Message createMessage(SourceData sourceData) {
        String msgWithMetaData = fillMetaData(sourceData.data);
        AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS, inlongGroupId, inlongStreamId,
                dataTime, 1, msgWithMetaData.length());
        String proxyPartitionKey = profile.get(PROXY_SEND_PARTITION_KEY, DigestUtils.md5Hex(inlongGroupId));
        Map<String, String> header = new HashMap<>();
        header.put(PROXY_KEY_DATA, proxyPartitionKey);
        header.put(OFFSET, sourceData.offset.toString());
        Message finalMsg = new DefaultMessage(msgWithMetaData.getBytes(StandardCharsets.UTF_8), header);
        // if the message size is greater than max pack size,should drop it.
        if (finalMsg.getBody().length > maxPackSize) {
            LOGGER.warn("message size is {}, greater than max pack size {}, drop it!",
                    finalMsg.getBody().length, maxPackSize);
            return null;
        }
        return finalMsg;
    }

    public String fillMetaData(String message) {
        if (!needMetadata) {
            return message;
        }
        long timestamp = System.currentTimeMillis();
        boolean isJson = FileDataUtils.isJSON(message);
        Map<String, String> mergeData = new HashMap<>(metadata);
        mergeData.put(DATA_CONTENT, FileDataUtils.getK8sJsonLog(message, isJson));
        mergeData.put(DATA_CONTENT_TIME, RECORD_TIME_FORMAT.format(new Date(timestamp)));
        return GSON.toJson(mergeData);
    }

    private boolean waitForPermit(String permitName, int permitLen) {
        boolean suc = false;
        while (!suc) {
            suc = MemoryManager.getInstance().tryAcquire(permitName, permitLen);
            if (!suc) {
                MemoryManager.getInstance().printDetail(permitName, "log file source");
                if (isInodeChanged() || !isRunnable()) {
                    return false;
                }
                AgentUtils.silenceSleepInSeconds(1);
            }
        }
        return true;
    }

    private boolean isInodeChanged() {
        if (AgentUtils.getCurrentTime() - lastInodeUpdateTime > INODE_UPDATE_INTERVAL_MS) {
            try {
                return FileDataUtils.getInodeInfo(fileName).compareTo(inodeInfo) != 0;
            } catch (IOException e) {
                LOGGER.error("check inode change file {} error {}", fileName, e.getMessage());
                return true;
            }
        }
        return false;
    }

    public Runnable coreThread() {
        return () -> {
            AgentThreadFactory.nameThread("log-file-source-" + taskId + "-" + file);
            running = true;
            long lastPrintTime = 0;
            while (isRunnable() && fileExist) {
                if (isInodeChanged()) {
                    fileExist = false;
                    LOGGER.info("inode changed, instance will restart and offset will be clean, file {}",
                            fileName);
                    break;
                }
                if (file.length() < bytePosition) {
                    fileExist = false;
                    LOGGER.info("file rotate, instance will restart and offset will be clean, file {}",
                            fileName);
                    break;
                }
                boolean suc = waitForPermit(AGENT_GLOBAL_READER_SOURCE_PERMIT, BATCH_READ_LINE_TOTAL_LEN);
                if (!suc) {
                    break;
                }
                List<SourceData> lines = null;
                try {
                    lines = readFromPos(bytePosition);
                } catch (FileNotFoundException e) {
                    fileExist = false;
                    LOGGER.error("readFromPos file deleted {}", e.getMessage());
                } catch (IOException e) {
                    LOGGER.error("readFromPos error {}", e.getMessage());
                }
                MemoryManager.getInstance().release(AGENT_GLOBAL_READER_SOURCE_PERMIT, BATCH_READ_LINE_TOTAL_LEN);
                if (lines.isEmpty()) {
                    if (queue.isEmpty()) {
                        emptyCount++;
                    } else {
                        emptyCount = 0;
                    }
                    AgentUtils.silenceSleepInSeconds(1);
                    continue;
                }
                emptyCount = 0;
                for (int i = 0; i < lines.size(); i++) {
                    boolean suc4Queue = waitForPermit(AGENT_GLOBAL_READER_QUEUE_PERMIT, lines.get(i).data.length());
                    if (!suc4Queue) {
                        break;
                    }
                    putIntoQueue(lines.get(i));
                }
                if (AgentUtils.getCurrentTime() - lastPrintTime > CORE_THREAD_PRINT_INTERVAL_MS) {
                    lastPrintTime = AgentUtils.getCurrentTime();
                    LOGGER.info("path is {}, linePosition {}, bytePosition is {} file len {}, reads lines size {}",
                            file.getName(), linePosition, bytePosition, file.length(), lines.size());
                }
            }
            running = false;
        };
    }

    private void putIntoQueue(SourceData sourceData) {
        try {
            boolean offerSuc = false;
            while (offerSuc != true) {
                offerSuc = queue.offer(sourceData, 1, TimeUnit.SECONDS);
            }
            LOGGER.debug("Read {} from file {}", sourceData.getData(), fileName);
        } catch (InterruptedException e) {
            if (sourceData != null) {
                MemoryManager.getInstance().release(AGENT_GLOBAL_READER_QUEUE_PERMIT, sourceData.data.length());
            }
            LOGGER.error("fetchData offer failed {}", e.getMessage());
        }
    }

    /**
     * Whether threads can in running state with while loop.
     *
     * @return true if threads can run
     */
    public boolean isRunnable() {
        return runnable;
    }

    /**
     * Stop running threads.
     */
    public void stopRunning() {
        runnable = false;
    }

    private List<SourceData> readFromPos(long pos) throws IOException {
        List<String> lines = new ArrayList<>();
        List<SourceData> dataList = new ArrayList<>();
        RandomAccessFile input = new RandomAccessFile(file, "r");
        bytePosition = readLines(input, pos, lines, BATCH_READ_LINE_COUNT, BATCH_READ_LINE_TOTAL_LEN, false);
        for (int i = 0; i < lines.size(); i++) {
            linePosition++;
            dataList.add(new SourceData(lines.get(i), linePosition));
        }
        if (input != null) {
            input.close();
        }
        return dataList;
    }

    @Override
    public void destroy() {
        LOGGER.info("destroy read source name {}", fileName);
        stopRunning();
        while (running) {
            AgentUtils.silenceSleepInMs(1);
        }
        clearQueue(queue);
        LOGGER.info("destroy read source name {} end", fileName);
    }

    private void clearQueue(BlockingQueue<SourceData> queue) {
        if (queue == null) {
            return;
        }
        while (queue != null && !queue.isEmpty()) {
            SourceData sourceData = null;
            try {
                sourceData = queue.poll(READ_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                LOGGER.warn("poll {} data get interrupted.", file.getPath(), e);
            }
            if (sourceData != null) {
                MemoryManager.getInstance().release(AGENT_GLOBAL_READER_QUEUE_PERMIT, sourceData.data.length());
            }
        }
        queue.clear();
    }

    @Override
    public boolean sourceFinish() {
        return emptyCount > EMPTY_CHECK_COUNT_AT_LEAST;
    }

    @Override
    public boolean sourceExist() {
        return fileExist;
    }

    @Override
    public List<Reader> split(TaskProfile jobConf) {
        return null;
    }
}
