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
 *
 */

package org.apache.inlong.sdk.sort.impl.interceptor;

import org.apache.inlong.sdk.sort.api.Interceptor;
import org.apache.inlong.sdk.sort.entity.InLongMessage;
import org.apache.inlong.sdk.sort.entity.InLongTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MsgTimeInterceptor implements Interceptor {
    private static final Logger logger = LoggerFactory.getLogger(MsgTimeInterceptor.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String KEY_SDK_START_TIME = "sortSdk.startTime";
    private static final String KEY_SDK_STOP_TIME = "sortSdk.stopTime";
    private static final long DEFAULT_START_TIME = 0L;
    private static final long DEFAULT_STOP_TIME = Long.MAX_VALUE;

    private long startTime;
    private long stopTime;

    public MsgTimeInterceptor(InLongTopic inLongTopic) {
        startTime = Optional.ofNullable(inLongTopic.getProperties().get(KEY_SDK_START_TIME))
                .map(s -> {
                    try {
                        return DATE_FORMAT.parse(s.toString()).getTime();
                    } catch (ParseException e) {
                        logger.error("parse start time failed, plz check the format of start time : {}", s);
                    }
                    return DEFAULT_START_TIME;
                })
                .orElse(DEFAULT_START_TIME);

        stopTime = Optional.ofNullable(inLongTopic.getProperties().get(KEY_SDK_STOP_TIME))
                .map(s -> {
                    logger.info("config TimeBasedFilterInterceptor, stop time is {}", s);
                    try {
                        return DATE_FORMAT.parse(s.toString()).getTime();
                    } catch (ParseException e) {
                        logger.error("parse stop time failed, plz check the format of stop time : {}", s);
                    }
                    return DEFAULT_STOP_TIME;
                })
                .orElse(DEFAULT_STOP_TIME);
    }

    @Override
    public List<InLongMessage> intercept(List<InLongMessage> messages) {
        return messages.stream()
                .filter(msg -> isValidMsgTime(msg.getMsgTime()))
                .collect(Collectors.toList());
    }

    private boolean isValidMsgTime(long msgTime) {
        return msgTime >= startTime && msgTime <= stopTime;
    }

}
