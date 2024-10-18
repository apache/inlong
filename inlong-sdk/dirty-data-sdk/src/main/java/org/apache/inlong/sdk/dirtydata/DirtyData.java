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

package org.apache.inlong.sdk.dirtydata;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Dirty data base class, it is a wrapper of dirty data
 */
public class DirtyData {

    private static final String DIRTY_TYPE_KEY = "DIRTY_TYPE";

    private static final String DIRTY_MESSAGE_KEY = "DIRTY_MESSAGE";
    private static final String SYSTEM_TIME_KEY = "SYSTEM_TIME";

    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * The identifier of dirty data, it will be used for filename generation of file dirty sink,
     * topic generation of mq dirty sink, tablename generation of database, etc,
     * and it supports variable replace like '${variable}'.
     * There are several system variables[SYSTEM_TIME|DIRTY_TYPE|DIRTY_MESSAGE] are currently supported,
     * and the support of other variables is determined by the connector.
     */
    private final String identifier;
    /**
     * The labels of the dirty data, it will be written to store system of dirty
     */
    private final String labels;
    /**
     * The log tag of dirty data, it is only used to format log as follows:
     * [${logTag}] ${labels} ${data}
     */
    private final String logTag;
    /**
     * Dirty type
     */
    private final String dirtyType;
    /**
     * Dirty describe message, it is the cause of dirty data
     */
    private final String dirtyMessage;
    /**
     * The real dirty data
     */
    private final byte[] data;

    public DirtyData(byte[] data, String identifier, String labels,
            String logTag, String dirtyType, String dirtyMessage) {
        this.data = data;
        this.dirtyType = dirtyType;
        this.dirtyMessage = dirtyMessage;
        Map<String, String> paramMap = genParamMap();
        this.labels = PatternReplaceUtils.replace(labels, paramMap);
        this.logTag = PatternReplaceUtils.replace(logTag, paramMap);
        this.identifier = PatternReplaceUtils.replace(identifier, paramMap);

    }

    public static Builder builder() {
        return new Builder();
    }

    private Map<String, String> genParamMap() {
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put(SYSTEM_TIME_KEY, DATE_TIME_FORMAT.format(LocalDateTime.now()));
        paramMap.put(DIRTY_TYPE_KEY, dirtyType);
        paramMap.put(DIRTY_MESSAGE_KEY, dirtyMessage);
        return paramMap;
    }

    public String getLabels() {
        return labels;
    }

    public String getLogTag() {
        return logTag;
    }

    public byte[] getData() {
        return data;
    }

    public String getDirtyType() {
        return dirtyType;
    }

    public String getIdentifier() {
        return identifier;
    }

    public static class Builder {

        private String identifier;
        private String labels;
        private String logTag;
        private String dirtyType = "UNDEFINED";
        private String dirtyMessage;
        private byte[] data;

        public Builder setDirtyType(String dirtyType) {
            this.dirtyType = dirtyType;
            return this;
        }

        public Builder setLabels(String labels) {
            this.labels = labels;
            return this;
        }

        public Builder setData(byte[] data) {
            this.data = data;
            return this;
        }

        public Builder setLogTag(String logTag) {
            this.logTag = logTag;
            return this;
        }

        public Builder setDirtyMessage(String dirtyMessage) {
            this.dirtyMessage = dirtyMessage;
            return this;
        }

        public DirtyData build() {
            return new DirtyData(data, identifier, labels, logTag, dirtyType, dirtyMessage);
        }
    }
}
