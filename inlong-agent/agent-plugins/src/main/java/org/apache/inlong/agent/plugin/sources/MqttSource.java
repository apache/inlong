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

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.TaskProfile;
import org.apache.inlong.agent.constant.CommonConstants;
import org.apache.inlong.agent.constant.TaskConstants;
import org.apache.inlong.agent.except.FileException;
import org.apache.inlong.agent.plugin.Message;
import org.apache.inlong.agent.plugin.file.Reader;
import org.apache.inlong.agent.plugin.sources.file.AbstractSource;
import org.apache.inlong.agent.plugin.sources.reader.MqttReader;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MqttSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(MqttSource.class);

    private MqttReader mqttReader;

    private String topic;

    public MqttSource() {
    }

    private List<Reader> splitSqlJob(String topics, String instanceId) {
        if (StringUtils.isEmpty(topics)) {
            return null;
        }
        final List<Reader> readers = new ArrayList<>();
        String[] topicList = topics.split(CommonConstants.COMMA);
        Arrays.stream(topicList).forEach(topic -> {
            MqttReader mqttReader = new MqttReader(topic);
            mqttReader.setReadSource(instanceId);
            readers.add(mqttReader);
        });
        return readers;
    }

    @Override
    public List<Reader> split(TaskProfile conf) {
        String topics = conf.get(TaskConstants.JOB_MQTT_TOPIC, StringUtils.EMPTY);
        List<Reader> readerList = splitSqlJob(topics, instanceId);
        if (CollectionUtils.isNotEmpty(readerList)) {
            sourceMetric.sourceSuccessCount.incrementAndGet();
        } else {
            sourceMetric.sourceFailCount.incrementAndGet();
        }
        return readerList;
    }

    @Override
    protected String getThreadName() {
        return null;
    }

    @Override
    protected void initSource(InstanceProfile profile) {
        try {
            LOGGER.info("MqttSource init: {}", profile.toJsonStr());
            topic = profile.get(TaskConstants.JOB_MQTT_TOPIC);
            mqttReader = new MqttReader(topic);
            mqttReader.init(profile);
        } catch (Exception e) {
            stopRunning();
        }
    }

    @Override
    protected void printCurrentState() {
        LOGGER.info("mqtt topic is {}", topic);
    }

    @Override
    protected boolean doPrepareToRead() {
        return true;
    }

    @Override
    protected List<SourceData> readFromSource() {
        List<SourceData> dataList = new ArrayList<>();
        try {
            int size = 0;
            while (size < BATCH_READ_LINE_TOTAL_LEN) {
                Message msg = read();
                if (msg != null) {
                    SourceData sourceData = new SourceData(msg.getBody(), "0L");
                    size += sourceData.getData().length;
                    dataList.add(sourceData);
                } else {
                    break;
                }
            }
        } catch (FileException e) {
            LOGGER.error("read from mqtt error", e);
        }
        return dataList;
    }

    @Override
    public Message read() {
        return mqttReader.read();
    }

    @Override
    protected boolean isRunnable() {
        return runnable;
    }

    @Override
    protected void releaseSource() {
        if (mqttReader != null) {
            mqttReader.destroy();
        }
    }

    @Override
    public boolean sourceFinish() {
        return mqttReader.isFinished();
    }

    @Override
    public boolean sourceExist() {
        return mqttReader.isSourceExist();
    }
}
