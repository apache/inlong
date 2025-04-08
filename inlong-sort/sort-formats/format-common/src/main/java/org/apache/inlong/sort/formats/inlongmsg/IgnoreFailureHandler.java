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

package org.apache.inlong.sort.formats.inlongmsg;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link FailureHandler} that ignores the failure.
 */
public class IgnoreFailureHandler implements FailureHandler {

    private static final Logger LOG = LoggerFactory.getLogger(IgnoreFailureHandler.class);

    @Override
    public void onParsingMsgFailure(Object msg, Exception exception) {
        LOG.error("Could not properly deserialize msg=[{}].", msg, exception);
    };

    @Override
    public void onParsingHeadFailure(String attribute, Exception exception) {
        LOG.warn("Cannot properly parse the head {}", attribute, exception);
    }

    @Override
    public void onParsingBodyFailure(InLongMsgHead head, byte[] body, Exception exception) {
        LOG.warn("Cannot properly parse the head: {}, body: {}.", head, new String(body), exception);
    }

    @Override
    public void onConvertingRowFailure(InLongMsgHead head, InLongMsgBody body, Exception exception) {
        LOG.warn("Cannot properly convert the InLongMsg ({}, {})", head, body, exception);
    }

    @Override
    public void onConvertingFieldFailure(String fieldName, String fieldText, FormatInfo formatInfo,
            Exception exception) throws Exception {
        LOG.warn("Cannot convert the InLongMsg Filed (fieldName = {}, formatInfo = {}, fieldText = {}),",
                fieldName, formatInfo, fieldText, exception);
    }

    @Override
    public boolean isIgnoreFailure() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o != null && getClass() == o.getClass();
    }
}
