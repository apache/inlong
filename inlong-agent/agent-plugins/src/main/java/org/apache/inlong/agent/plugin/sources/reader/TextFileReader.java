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

package org.apache.inlong.agent.plugin.sources.reader;

import static org.apache.inlong.agent.constants.CommonConstants.*;
import static org.apache.inlong.agent.constants.JobConstants.DEFAULT_JOB_FILE_MAX_WAIT;
import static org.apache.inlong.agent.constants.JobConstants.JOB_FILE_MAX_WAIT;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.Reader;
import org.apache.inlong.agent.plugin.Validator;
import org.apache.inlong.agent.plugin.except.FileException;
import org.apache.inlong.agent.plugin.metrics.PluginJmxMetric;
import org.apache.inlong.agent.plugin.metrics.PluginMetric;
import org.apache.inlong.agent.plugin.metrics.PluginPrometheusMetric;
import org.apache.inlong.agent.plugin.validator.PatternValidator;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextFileReader implements Reader {

    private static final Logger LOGGER = LoggerFactory.getLogger(TextFileReader.class);

    private static final String TEXT_FILE_READER_TAG_NAME = "AgentTextMetric";

    public static final int NEVER_STOP_SIGN = -1;

    private String inlongGroupId;
    private String inlongStreamId;
    private final File file;
    private final int position;
    private final String md5;
    private Iterator<String> iterator;
    private Stream<String> stream;
    private long timeout;
    private long waitTimeout;
    private long lastTime = 0;
    private final PluginMetric textFileMetric;
    private List<Validator> validators = new ArrayList<>();
    private static AtomicLong metricsIndex = new AtomicLong(0);

    public TextFileReader(File file, int position) {
        this(file, position, "");
    }

    public TextFileReader(File file, int position, String md5) {
        this.file = file;
        this.position = position;
        this.md5 = md5;

        if (ConfigUtil.isPrometheusEnabled()) {
            textFileMetric = new PluginPrometheusMetric(AgentUtils.getUniqId(
                    TEXT_FILE_READER_TAG_NAME, metricsIndex.incrementAndGet()));
        } else {
            textFileMetric = new PluginJmxMetric(AgentUtils.getUniqId(
                    TEXT_FILE_READER_TAG_NAME, metricsIndex.incrementAndGet()));
        }
    }

    public TextFileReader(File file) {
        this(file, 0);
    }

    @Override
    public Message read() {
        if (iterator != null && iterator.hasNext()) {
            String message = iterator.next();
            if (validateMessage(message)) {
                AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_READ_SUCCESS,
                        inlongGroupId, inlongStreamId, System.currentTimeMillis());
                textFileMetric.incReadNum();
                return new DefaultMessage(message.getBytes(StandardCharsets.UTF_8));
            }
        }
        AgentUtils.silenceSleepInMs(waitTimeout);
        return null;
    }

    private boolean validateMessage(String message) {
        if (validators.isEmpty()) {
            return true;
        }
        return validators.stream().allMatch(v -> v.validate(message));
    }

    @Override
    public boolean isFinished() {
        if (timeout == NEVER_STOP_SIGN) {
            return false;
        }
        if (iterator == null) {
            return true;
        }
        if (iterator.hasNext()) {
            lastTime = 0;
            return false;
        } else {
            if (lastTime == 0) {
                lastTime = System.currentTimeMillis();
            }
            return System.currentTimeMillis() - lastTime > timeout;
        }
    }

    @Override
    public String getReadSource() {
        return file.getAbsolutePath();
    }

    @Override
    public void setReadTimeout(long millis) {
        timeout = millis;
    }

    @Override
    public void setWaitMillisecs(long millis) {
        waitTimeout = millis;
    }

    public void addPatternValidator(String pattern) {
        if (pattern.isEmpty()) {
            return;
        }
        validators.add(new PatternValidator(pattern));
    }

    @Override
    public void init(JobProfile jobConf) {
        try {
            initReadTimeout(jobConf);
            String md5 = AgentUtils.getFileMd5(file);
            if (StringUtils.isNotBlank(this.md5) && !this.md5.equals(md5)) {
                LOGGER.warn("md5 is differ from origin, origin: {}, new {}", this.md5, md5);
            }
            LOGGER.info("file name for task is {}, md5 is {}", file, md5);
            stream = Files.newBufferedReader(file.toPath()).lines().skip(position);
            iterator = stream.iterator();
            inlongGroupId = jobConf.get(PROXY_INLONG_GROUP_ID, DEFAULT_PROXY_INLONG_GROUP_ID);
            inlongStreamId = jobConf.get(PROXY_INLONG_STREAM_ID, DEFAULT_PROXY_INLONG_STREAM_ID);
        } catch (Exception ex) {
            throw new FileException("error init stream for " + file.getPath(), ex);
        }
    }

    private void initReadTimeout(JobProfile jobConf) {
        int waitTime = jobConf.getInt(JOB_FILE_MAX_WAIT,
                DEFAULT_JOB_FILE_MAX_WAIT);
        if (waitTime == NEVER_STOP_SIGN) {
            timeout = NEVER_STOP_SIGN;
        } else {
            timeout = TimeUnit.MINUTES.toMillis(waitTime);
        }
    }

    @Override
    public void destroy() {
        AgentUtils.finallyClose(stream);
        LOGGER.info("destroy reader with read {} num {}",
                textFileMetric.getTagName(), textFileMetric.getReadNum());
    }
}
