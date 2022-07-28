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

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.message.DefaultMessage;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.SourceMeta;
import org.apache.inlong.agent.plugin.Validator;
import org.apache.inlong.agent.plugin.except.FileException;
import org.apache.inlong.agent.plugin.metadata.AgentMetadata;
import org.apache.inlong.agent.plugin.metadata.KubernetesMetadata;
import org.apache.inlong.agent.plugin.sources.snapshot.LocalSnapshot;
import org.apache.inlong.agent.plugin.validator.PatternValidator;
import org.apache.inlong.agent.utils.AgentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.inlong.agent.constant.AgentConstants.GLOBAL_METRICS;
import static org.apache.inlong.agent.constant.CommonConstants.POSITION_SUFFIX;
import static org.apache.inlong.agent.constant.JobConstants.DEFAULT_JOB_FILE_MAX_WAIT;
import static org.apache.inlong.agent.constant.JobConstants.JOB_FILE_DB_OFFSETS;
import static org.apache.inlong.agent.constant.JobConstants.JOB_FILE_LINE_END_PATTERN;
import static org.apache.inlong.agent.constant.JobConstants.JOB_FILE_MAX_WAIT;
import static org.apache.inlong.agent.constant.JobConstants.JOB_META_INFO_LIST;
import static org.apache.inlong.agent.constant.JobConstants.JOB_OFFSET_DELIMITER;
import static org.apache.inlong.agent.constant.JobConstants.JOB_OFFSET_DELIMITER_TASK;
import static org.apache.inlong.agent.plugin.utils.MetaDataUtils.getLogInfo;

/**
 * read file data
 */
public class TextFileReader extends AbstractReader {

    public static final int NEVER_STOP_SIGN = -1;
    private static final Logger LOGGER = LoggerFactory.getLogger(TextFileReader.class);
    private static final String TEXT_FILE_READER_TAG_NAME = "AgentTextMetric";
    private final File file;

    private final int position;

    private final String md5;
    private final Map<File, String> lineStringBuffer = new ConcurrentHashMap<>();
    private Iterator<String> iterator;

    private Stream<String> stream;

    private long timeout;

    private long waitTimeout;

    private long lastTime = 0;
    
    private LocalSnapshot localSnapshot;
    
    private List<SourceMeta> sourceMetas;
    
    private List<Validator> validators = new ArrayList<>();

    public TextFileReader(File file, int position, List<SourceMeta> sourceMetas) {
        this(file, position, "");
        this.sourceMetas = sourceMetas;
    }

    public TextFileReader(File file, int position) {
        this(file, position, "");
    }

    public TextFileReader(File file, int position, String md5) {
        this.file = file;
        this.position = position;
        this.md5 = md5;
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
                GLOBAL_METRICS.incReadNum(metricTagName);
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
    public void setWaitMillisecond(long millis) {
        waitTimeout = millis;
    }

    @Override
    public String getSnapshot() {
        if (Objects.nonNull(localSnapshot)) {
            return file.getPath() + JOB_OFFSET_DELIMITER_TASK + localSnapshot.getSnapshot();
        } else {
            return "";
        }
    }

    private void setSnapshot(JobProfile jobConf) {
        String snapshots = jobConf.get(JOB_FILE_DB_OFFSETS, null);
        if (Objects.isNull(snapshots)) {
            return;
        }
        String[] taskAndSnapshot = snapshots.split(JOB_OFFSET_DELIMITER);
        for (String sna : taskAndSnapshot) {
            String[] snapshot = sna.split(JOB_OFFSET_DELIMITER_TASK);
            if (snapshot.length > 1 && file.getPath().equalsIgnoreCase(snapshot[0])) {
                localSnapshot.save(snapshot[1]);
                break;
            }
        }
    }

    @Override
    public void finishRead() {
        destroy();
    }

    @Override
    public boolean isSourceExist() {
        return true;
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
            super.init(jobConf);
            metricTagName = TEXT_FILE_READER_TAG_NAME + "_" + inlongGroupId;
            initReadTimeout(jobConf);
            String md5 = AgentUtils.getFileMd5(file);
            if (StringUtils.isNotBlank(this.md5) && !this.md5.equals(md5)) {
                LOGGER.warn("md5 is differ from origin, origin: {}, new {}", this.md5, md5);
            }
            localSnapshot = new LocalSnapshot(file.getPath().concat(POSITION_SUFFIX));
            setSnapshot(jobConf);
            LOGGER.info("file name for task is {}, md5 is {}", file, md5);
            //split line and column
            getFileStream(jobConf);
            if (Objects.nonNull(stream)) {
                iterator = stream.iterator();
            }
        } catch (Exception ex) {
            throw new FileException("error init stream for " + file.getPath(), ex);
        }
    }

    private void getFileStream(JobProfile jobConf) throws IOException {
        List<String> lines = Files.newBufferedReader(file.toPath()).lines().skip(position).collect(Collectors.toList());
        List<String> resultLines = new ArrayList<>();
        //TODO line regular expression matching
        if (jobConf.hasKey(JOB_FILE_LINE_END_PATTERN)) {
            Pattern pattern = Pattern.compile(jobConf.get(JOB_FILE_LINE_END_PATTERN));
            lines.forEach(line -> {
                lineStringBuffer.put(file,
                        lineStringBuffer.isEmpty() ? line : lineStringBuffer.get(file).concat(" ").concat(line));
                String data = lineStringBuffer.get(file);
                Matcher matcher = pattern.matcher(data);
                if (matcher.find() && StringUtils.isNoneBlank(matcher.group())) {
                    String[] splitLines = data.split(matcher.group());
                    int length = splitLines.length;
                    for (int i = 0; i < length; i++) {
                        if (i > 0 && i == length - 1 && null != splitLines[i]) {
                            lineStringBuffer.put(file, splitLines[i]);
                            break;
                        }
                        resultLines.add(splitLines[i]);
                    }
                    if (0 == length - 1) {
                        lineStringBuffer.remove(file);
                    }
                }
            });
            if (resultLines.isEmpty()) {
                return;
            }
        }
        lines = resultLines.isEmpty() ? lines : resultLines;
        stream = addAttributeData(jobConf, lines);
    }

    private Stream<String> addAttributeData(JobProfile jobConf, List<String> lines) {
        if (!jobConf.hasKey(JOB_META_INFO_LIST)) {
            return lines.stream();
        }
        lines = joinMetaAndLine(sourceMetas, lines);
        return lines.stream();
    }

    private List<String> joinMetaAndLine(List<SourceMeta> sourceMetas, List<String> lines) {
        if (Objects.isNull(sourceMetas)) {
            return lines;
        }
        for (SourceMeta sm : sourceMetas) {
            if (sm instanceof KubernetesMetadata) {
                return joinMetaK8s((KubernetesMetadata) sm, lines);
            }
            if (sm instanceof AgentMetadata) {
                joinMetaAgent((AgentMetadata) sm, lines);
            }
        }
        return lines;
    }

    //TODO agent information
    private void joinMetaAgent(AgentMetadata sm, List<String> lines) {
    }

    private List<String> joinMetaK8s(KubernetesMetadata sourceMeta, List<String> lines) {
        Map<String, String> podInfo = getLogInfo(file.getName());
        if (podInfo.isEmpty()) {
            return lines;
        }
        String podMetadata = sourceMeta
                .getPodMetadata(podInfo.get(CommonConstants.NAMESPACE), podInfo.get(CommonConstants.POD_NAME))
                .toString();
        return lines.stream().parallel().map(data -> podMetadata.concat("\n").concat(data))
                .collect(Collectors.toList());
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
        if (stream == null) {
            return;
        }
        AgentUtils.finallyClose(stream);
        LOGGER.info("destroy reader with read {} num {}",
                metricTagName, GLOBAL_METRICS.getReadNum(metricTagName));
    }
}
