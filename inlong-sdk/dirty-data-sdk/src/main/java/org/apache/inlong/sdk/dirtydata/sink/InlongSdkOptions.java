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

package org.apache.inlong.sdk.dirtydata.sink;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;

@Data
@Builder
@Getter
public class InlongSdkOptions implements Serializable {

    private static final String DEFAULT_FORMAT = "csv";

    private static final String DEFAULT_CSV_FIELD_DELIMITER = ",";
    private static final String DEFAULT_CSV_LINE_DELIMITER = "\n";

    private static final String DEFAULT_KV_FIELD_DELIMITER = "&";
    private static final String DEFAULT_KV_ENTRY_DELIMITER = "=";

    private String inlongGroupId;
    private String inlongStreamId;
    private String inlongManagerAddr;
    private String inlongManagerAuthKey;
    private String inlongManagerAuthId;
    private String format = DEFAULT_FORMAT;
    private boolean ignoreSideOutputErrors;
    private boolean enableDirtyLog;
    private String csvFieldDelimiter = DEFAULT_CSV_FIELD_DELIMITER;
    private String csvLineDelimiter = DEFAULT_CSV_LINE_DELIMITER;
    private String kvFieldDelimiter = DEFAULT_KV_FIELD_DELIMITER;
    private String kvEntryDelimiter = DEFAULT_KV_ENTRY_DELIMITER;
}
