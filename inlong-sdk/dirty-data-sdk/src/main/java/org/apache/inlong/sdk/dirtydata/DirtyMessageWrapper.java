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

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.text.StringEscapeUtils;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.StringJoiner;

@Slf4j
@Builder
public class DirtyMessageWrapper {

    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private String delimiter;

    private String inlongGroupId;
    private String inlongStreamId;
    private long dataTime;
    private String dataflowId;
    private String serverType;
    private String dirtyType;
    private String dirtyMessage;
    private String ext;
    private String data;
    private byte[] dataBytes;

    public String format() {
        String reportTime = LocalDateTime.now().format(dateTimeFormatter);
        StringJoiner joiner = new StringJoiner(delimiter);
        String formatData = null;
        if (data != null) {
            formatData = data;
        } else if (dataBytes != null) {
            formatData = Base64.getEncoder().encodeToString(dataBytes);
        }

        String dataTimeStr = LocalDateTime.ofInstant(Instant.ofEpochMilli(dataTime),
                ZoneId.systemDefault()).format(dateTimeFormatter);
        return joiner
                .add(dataflowId)
                .add(inlongGroupId)
                .add(inlongStreamId)
                .add(reportTime)
                .add(dataTimeStr)
                .add(serverType)
                .add(dirtyType)
                .add(StringEscapeUtils.escapeXSI(dirtyMessage))
                .add(StringEscapeUtils.escapeXSI(ext))
                .add(StringEscapeUtils.escapeXSI(formatData))
                .toString();
    }
}
