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

import org.apache.inlong.sdk.dirtydata.sink.Configure;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;

import static org.apache.inlong.sdk.dirtydata.Constants.DIRTY_COLLECT_ENABLE;
import static org.apache.inlong.sdk.dirtydata.Constants.DIRTY_SIDE_OUTPUT_CONNECTOR;
import static org.apache.inlong.sdk.dirtydata.Constants.DIRTY_SIDE_OUTPUT_IGNORE_ERRORS;
import static org.apache.inlong.sdk.dirtydata.Constants.DIRTY_SIDE_OUTPUT_LABELS;
import static org.apache.inlong.sdk.dirtydata.Constants.DIRTY_SIDE_OUTPUT_LOG_TAG;

/**
 * Dirty common options
 */
@Data
@Builder
@Getter
public class DirtyOptions implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final String DEFAULT_FORMAT = "csv";
    private static final String DEFAULT_CSV_FIELD_DELIMITER = ",";
    private final boolean enableDirtyCollect;
    private final boolean ignoreSideOutputErrors;
    private final String sinkType;
    private final String labels;
    private final String logTag;
    private final String format;
    private final String csvFieldDelimiter;

    private DirtyOptions(boolean enableDirtyCollect, boolean ignoreSideOutputErrors,
            String sinkType, String labels, String logTag, String format, String csvFieldDelimiter) {
        this.enableDirtyCollect = enableDirtyCollect;
        this.ignoreSideOutputErrors = ignoreSideOutputErrors;
        this.sinkType = sinkType;
        this.labels = labels;
        this.logTag = logTag;
        this.format = format;
        this.csvFieldDelimiter = csvFieldDelimiter;
    }

    /**
     * Get dirty options from {@link Configure}
     *
     * @param config The config
     * @return Dirty options
     */
    public static DirtyOptions fromConfig(Configure config) {
        boolean enableDirtyCollect = config.getBoolean(DIRTY_COLLECT_ENABLE, false);
        boolean ignoreSinkError = config.getBoolean(DIRTY_SIDE_OUTPUT_IGNORE_ERRORS, true);
        String dirtyConnector = config.get(DIRTY_SIDE_OUTPUT_CONNECTOR, null);
        String labels = config.get(DIRTY_SIDE_OUTPUT_LABELS, null);
        String logTag = config.get(DIRTY_SIDE_OUTPUT_LOG_TAG, "DirtyData");
        String format = DEFAULT_FORMAT;
        String csvFieldDelimiter = DEFAULT_CSV_FIELD_DELIMITER;
        return new DirtyOptions(enableDirtyCollect, ignoreSinkError,
                dirtyConnector, labels, logTag, format, csvFieldDelimiter);
    }

    public void validate() {
        if (!enableDirtyCollect) {
            return;
        }
        if (sinkType == null || sinkType.trim().length() == 0) {
            throw new RuntimeException(
                    "The option 'dirty.side-output.connector' is not allowed to be empty "
                            + "when the option 'dirty.ignore' is 'true' "
                            + "and the option 'dirty.side-output.enable' is 'true'");
        }
    }
}
