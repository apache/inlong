/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.standalone.channel;

import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.flume.event.SimpleEvent;
import org.apache.inlong.sort.standalone.config.pojo.InlongId;
import org.apache.inlong.sort.standalone.utils.Constants;

/**
 * 
 * ProfileEvent
 */
public class ProfileEvent extends SimpleEvent {

    private final String inlongGroupId;
    private final String inlongStreamId;
    private final String uid;

    private final long rawLogTime;
    private final long fetchTime;
    private long sendTime;

    /**
     * Constructor
     * 
     * @param body
     * @param headers
     */
    public ProfileEvent(byte[] body, Map<String, String> headers) {
        super.setBody(body);
        super.setHeaders(headers);
        this.inlongGroupId = headers.get(Constants.INLONG_GROUP_ID);
        this.inlongStreamId = headers.get(Constants.INLONG_STREAM_ID);
        this.uid = InlongId.generateUid(inlongGroupId, inlongStreamId);
        this.fetchTime = System.currentTimeMillis();
        this.sendTime = fetchTime;
        this.rawLogTime = NumberUtils.toLong(headers.get(Constants.HEADER_KEY_MSG_TIME), fetchTime);
    }

    /**
     * get sendTime
     * 
     * @return the sendTime
     */
    public long getSendTime() {
        return sendTime;
    }

    /**
     * set sendTime
     * 
     * @param sendTime the sendTime to set
     */
    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
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
     * get inlongStreamId
     * 
     * @return the inlongStreamId
     */
    public String getInlongStreamId() {
        return inlongStreamId;
    }

    /**
     * get rawLogTime
     * 
     * @return the rawLogTime
     */
    public long getRawLogTime() {
        return rawLogTime;
    }

    /**
     * get fetchTime
     * 
     * @return the fetchTime
     */
    public long getFetchTime() {
        return fetchTime;
    }

    /**
     * get uid
     * 
     * @return the uid
     */
    public String getUid() {
        return uid;
    }

}
