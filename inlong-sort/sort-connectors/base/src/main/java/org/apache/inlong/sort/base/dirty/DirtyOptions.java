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

package org.apache.inlong.sort.base.dirty;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import java.io.Serializable;
import static org.apache.inlong.sort.base.Constants.DIRTY_IGNORE;
import static org.apache.inlong.sort.base.Constants.DIRTY_SINK_ENABLE;
import static org.apache.inlong.sort.base.Constants.SINK_DIRTY_CONNECTOR;
import static org.apache.inlong.sort.base.Constants.SINK_DIRTY_IGNORE_SINK_ERRORS;
import static org.apache.inlong.sort.base.Constants.SINK_DIRTY_LABELS;
import static org.apache.inlong.sort.base.Constants.SINK_DIRTY_LOG_TAG;

/**
 * Dirty common options
 */
public class DirtyOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean ignoreDirty;
    private final boolean enableDirtySink;
    private final boolean ignoreSinkErrors;
    private final String dirtyConnector;
    private final String labels;
    private final String logTag;

    private DirtyOptions(boolean ignoreDirty, boolean enableDirtySink, boolean ignoreSinkErrors,
            String dirtyConnector, String labels, String logTag) {
        this.ignoreDirty = ignoreDirty;
        this.enableDirtySink = enableDirtySink;
        this.ignoreSinkErrors = ignoreSinkErrors;
        this.dirtyConnector = dirtyConnector;
        this.labels = labels;
        this.logTag = logTag;
    }

    /**
     * Get dirty options from {@link ReadableConfig}
     *
     * @param config The config
     * @return Dirty options
     */
    public static DirtyOptions fromConfig(ReadableConfig config) {
        boolean ignoreDirty = config.get(DIRTY_IGNORE);
        boolean enableDirtySink = config.get(DIRTY_SINK_ENABLE);
        boolean ignoreSinkError = config.get(SINK_DIRTY_IGNORE_SINK_ERRORS);
        String dirtyConnector = config.getOptional(SINK_DIRTY_CONNECTOR).orElse(null);
        String labels = config.getOptional(SINK_DIRTY_LABELS).orElse(null);
        String logTag = config.get(SINK_DIRTY_LOG_TAG);
        return new DirtyOptions(ignoreDirty, enableDirtySink, ignoreSinkError, dirtyConnector, labels, logTag);
    }

    public void validate() {
        if (!ignoreDirty || !enableDirtySink) {
            return;
        }
        if (dirtyConnector == null || dirtyConnector.length() == 0) {
            throw new ValidationException(
                    "The option 'sink.dirty.connector' is not allowed to be empty "
                            + "when the option 'dirty.ignore' is 'true' and the option 'dirty.sink.enable' is 'true'");
        }
    }

    public boolean ignoreDirty() {
        return ignoreDirty;
    }

    public boolean enableDirtySink() {
        return enableDirtySink;
    }

    public String getDirtyConnector() {
        return dirtyConnector;
    }

    public String getLabels() {
        return labels;
    }

    public String getLogTag() {
        return logTag;
    }

    public boolean ignoreSinkErrors() {
        return ignoreSinkErrors;
    }
}
