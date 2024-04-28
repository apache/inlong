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

package org.apache.inlong.sort.standalone.sink.pulsar;

import org.apache.inlong.common.enums.DataTypeEnum;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.utils.Constants;

import java.util.Map;

/**
 *
 * KafkaIdConfig
 */
public class PulsarIdConfig {

    public static final String KEY_DATA_TYPE = "dataType";
    public static final String KEY_SEPARATOR = "separator";
    public static final String DEFAULT_SEPARATOR = "|";

    private static final String DEFAULT_INLONG_STREAM = "1";

    private String inlongGroupId;
    private String inlongStreamId;
    private String uid;
    private String separator = "|";
    private String topic;
    private DataTypeEnum dataType = DataTypeEnum.TEXT;

    /**
     * Constructor
     */
    public PulsarIdConfig() {

    }

    /**
     * Constructor
     *
     * @param idParam
     */
    public PulsarIdConfig(Map<String, String> idParam) {
        this.inlongGroupId = idParam.get(Constants.INLONG_GROUP_ID);
        this.inlongStreamId = idParam.getOrDefault(Constants.INLONG_STREAM_ID, DEFAULT_INLONG_STREAM);
        this.uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
        this.separator = idParam.getOrDefault(PulsarIdConfig.KEY_SEPARATOR, PulsarIdConfig.DEFAULT_SEPARATOR);
        this.topic = idParam.getOrDefault(Constants.TOPIC, uid);
        this.dataType = DataTypeEnum
                .convert(idParam.getOrDefault(PulsarIdConfig.KEY_DATA_TYPE, DataTypeEnum.TEXT.getType()));
    }

    /**
     * get inlongGroupId
     *
     * @return the inlongGroupId
     */
    public String getInlongGroupId() {
        return inlongGroupId;
    }

    /**
     * set inlongGroupId
     *
     * @param inlongGroupId the inlongGroupId to set
     */
    public void setInlongGroupId(String inlongGroupId) {
        this.inlongGroupId = inlongGroupId;
    }

    /**
     * get inlongStreamId
     *
     * @return the inlongStreamId
     */
    public String getInlongStreamId() {
        return inlongStreamId;
    }

    /**
     * set inlongStreamId
     *
     * @param inlongStreamId the inlongStreamId to set
     */
    public void setInlongStreamId(String inlongStreamId) {
        this.inlongStreamId = inlongStreamId;
    }

    /**
     * get uid
     *
     * @return the uid
     */
    public String getUid() {
        return uid;
    }

    /**
     * set uid
     *
     * @param uid the uid to set
     */
    public void setUid(String uid) {
        this.uid = uid;
    }

    /**
     * get separator
     *
     * @return the separator
     */
    public String getSeparator() {
        return separator;
    }

    /**
     * set separator
     *
     * @param separator the separator to set
     */
    public void setSeparator(String separator) {
        this.separator = separator;
    }

    /**
     * get topic
     *
     * @return the topic
     */
    public String getTopic() {
        return topic;
    }

    /**
     * set topic
     *
     * @param topic the topic to set
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * get dataType
     *
     * @return the dataType
     */
    public DataTypeEnum getDataType() {
        return dataType;
    }

    /**
     * set dataType
     *
     * @param dataType the dataType to set
     */
    public void setDataType(DataTypeEnum dataType) {
        this.dataType = dataType;
    }

}
