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
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.core.task.MemoryManager;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.plugin.sources.extend.DefaultExtendedHandler;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ThreadUtils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.inlong.agent.constant.TaskConstants.SQL_TASK_DATA_SEPARATOR;
import static org.apache.inlong.agent.constant.TaskConstants.SQL_TASK_FETCH_SIZE;

/**
 * Read by SQL
 */
public class SQLSource extends AbstractSource {

    public static final String AGENT_GLOBAL_SQL_SOURCE_PERMIT = "agent.global.sql.source.permit";
    public static final int DEFAULT_AGENT_GLOBAL_SQL_SOURCE_PERMIT = 128 * 1000 * 1000;
    private int MAX_RECONNECT_TIMES = 3;
    private int RECONNECT_INTERVAL_SECOND = 10;
    private int DEFAULT_FETCH_SIZE = 1000;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected class FileOffset {

        private Long lineOffset;
        private Long byteOffset;
        private boolean hasByteOffset;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(SQLSource.class);
    public static final String OFFSET_SEP = ":";
    protected final Integer WAIT_TIMEOUT_MS = 10;
    private final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private String fileName;
    private String jdbcUrl;
    private String username;
    private String password;
    private Connection conn;
    protected BlockingQueue<SourceData> queue;
    private static final ThreadPoolExecutor EXECUTOR_SERVICE = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE,
            1L, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new AgentThreadFactory("sql-source-pool"));
    private volatile boolean running = false;
    private boolean isMysql = true;
    private volatile boolean fileExist = true;

    public SQLSource() {
    }

    @Override
    protected void initExtendClass() {
        extendClass = DefaultExtendedHandler.class.getCanonicalName();
    }

    @Override
    protected void initSource(InstanceProfile profile) {
        try {
            LOGGER.info("sql source init: {}", profile.toJsonStr());
            AgentConfiguration conf = AgentConfiguration.getAgentConf();
            int permit = conf.getInt(AGENT_GLOBAL_SQL_SOURCE_PERMIT, DEFAULT_AGENT_GLOBAL_SQL_SOURCE_PERMIT);
            MemoryManager.getInstance().addSemaphore(AGENT_GLOBAL_SQL_SOURCE_PERMIT, permit);
            fileName = profile.getInstanceId();
            jdbcUrl = profile.get(TaskConstants.SQL_TASK_JDBC_URL).trim().replace("\r", "")
                    .replace("\n", "");
            if (jdbcUrl.startsWith("jdbc:mysql:")) {
                isMysql = true;
            }
            username = profile.get(TaskConstants.SQL_TASK_USERNAME);
            password = profile.get(TaskConstants.SQL_TASK_PASSWORD);

            queue = new LinkedBlockingQueue<>(CACHE_QUEUE_SIZE);
            initConn();
            EXECUTOR_SERVICE.execute(run());
        } catch (Exception ex) {
            stopRunning();
            throw new FileException("error init stream for " + fileName, ex);
        }
    }

    private void initConn() throws SQLException {
        int retryTimes = 0;
        while (conn == null) {
            try {
                conn = DriverManager.getConnection(jdbcUrl, username, password);
            } catch (Exception e) {
                retryTimes++;
                if (retryTimes >= MAX_RECONNECT_TIMES) {
                    throw new SQLException(
                            "Failed to connect database after retry " + retryTimes + " times.", e);
                }
                LOGGER.warn(
                        "Reconnect database after "
                                + RECONNECT_INTERVAL_SECOND
                                + " seconds due to the following error: "
                                + e.getMessage());
                AgentUtils.silenceSleepInSeconds(RECONNECT_INTERVAL_SECOND);
            }
        }

    }

    @Override
    protected boolean doPrepareToRead() {

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
        MemoryManager.getInstance().release(AGENT_GLOBAL_SQL_SOURCE_PERMIT, len);
        return lines;
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("path is {}, linePosition {}, bytePosition is {} file len {}");
    }

    @Override
    protected String getThreadName() {
        return "cos-file-source-" + taskId + "-" + fileName;
    }

    private Runnable run() {
        return () -> {
            AgentThreadFactory.nameThread(getThreadName());
            running = true;
            try {
                doRun();
            } catch (Throwable e) {
                fileExist = false;
                LOGGER.error("do run error maybe connect broken: ", e);
                ThreadUtils.threadThrowableHandler(Thread.currentThread(), e);
            }
            running = false;
        };
    }

    protected void doRun() throws SQLException, IOException {
        try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            if (isMysql) {
                stmt.setFetchSize(Integer.MIN_VALUE);
            } else {
                stmt.setFetchSize(profile.getInt(SQL_TASK_FETCH_SIZE, DEFAULT_FETCH_SIZE));
            }
            try (ResultSet rs = stmt.executeQuery(profile.getInstanceId())) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                ByteArrayOutputStream bas = new ByteArrayOutputStream();
                byte[] sep = profile.get(SQL_TASK_DATA_SEPARATOR).getBytes(StandardCharsets.UTF_8);
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        if (i > 1) {
                            bas.write(sep);
                        }
                        bas.write(rs.getBytes(i));
                    }
                    SourceData sourceData = new SourceData(bas.toByteArray(),
                            getOffsetString(0L, 0L));
                    boolean suc = false;
                    while (isRunnable() && !suc) {
                        boolean suc4Queue = waitForPermit(AGENT_GLOBAL_SQL_SOURCE_PERMIT, sourceData.getData().length);
                        if (!suc4Queue) {
                            break;
                        }
                        suc = queue.offer(sourceData);
                        if (!suc) {
                            MemoryManager.getInstance()
                                    .release(AGENT_GLOBAL_SQL_SOURCE_PERMIT, sourceData.getData().length);
                            AgentUtils.silenceSleepInMs(WAIT_TIMEOUT_MS);
                        }
                    }
                    bas.reset();
                }
            }
        } catch (SQLException e) {
            LOGGER.error("read from result set error: ", e);
            throw e;
        } catch (IOException e) {
            LOGGER.error("read from result io error: ", e);
            throw e;
        }
    }

    private String getOffsetString(Long lineOffset, Long byteOffset) {
        return lineOffset + OFFSET_SEP + byteOffset;
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
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOGGER.error("close connect error: ", e);
            }
        }
        while (!queue.isEmpty()) {
            MemoryManager.getInstance().release(AGENT_GLOBAL_SQL_SOURCE_PERMIT, queue.poll().getData().length);
        }
    }

    private boolean waitForPermit(String permitName, int permitLen) {
        boolean suc = false;
        while (!suc) {
            suc = MemoryManager.getInstance().tryAcquire(permitName, permitLen);
            if (!suc) {
                MemoryManager.getInstance().printDetail(permitName, "sql_source");
                if (!isRunnable()) {
                    return false;
                }
                AgentUtils.silenceSleepInMs(WAIT_TIMEOUT_MS);
            }
        }
        return true;
    }
}
