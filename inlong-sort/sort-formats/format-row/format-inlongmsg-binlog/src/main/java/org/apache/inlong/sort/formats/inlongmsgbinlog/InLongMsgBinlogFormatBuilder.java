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

package org.apache.inlong.sort.formats.inlongmsgbinlog;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;

import org.apache.flink.table.descriptors.DescriptorProperties;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.DEFAULT_INCLUDE_UPDATE_BEFORE;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.DEFAULT_METADATA_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.FORMAT_INCLUDE_UPDATE_BEFORE;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.FORMAT_METADATA_FIELD_NAME;

public abstract class InLongMsgBinlogFormatBuilder<T extends InLongMsgBinlogFormatBuilder> {

    protected final RowFormatInfo rowFormatInfo;

    protected String timeFieldName = DEFAULT_TIME_FIELD_NAME;
    protected String attributesFieldName = DEFAULT_ATTRIBUTES_FIELD_NAME;
    protected String metadataFieldName = DEFAULT_METADATA_FIELD_NAME;
    protected boolean ignoreErrors = DEFAULT_IGNORE_ERRORS;
    protected boolean includeUpdateBefore = DEFAULT_INCLUDE_UPDATE_BEFORE;

    protected InLongMsgBinlogFormatBuilder(RowFormatInfo rowFormatInfo) {
        this.rowFormatInfo = rowFormatInfo;
    }

    @SuppressWarnings("unchecked")
    public T setTimeFieldName(String timeFieldName) {
        this.timeFieldName = timeFieldName;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setAttributesFieldName(String attributesFieldName) {
        this.attributesFieldName = attributesFieldName;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setMetadataFieldName(String metadataFieldName) {
        this.metadataFieldName = metadataFieldName;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setIncludeUpdateBefore(boolean includeUpdateBefore) {
        this.includeUpdateBefore = includeUpdateBefore;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T configure(DescriptorProperties descriptorProperties) {
        descriptorProperties.getOptionalString(FORMAT_TIME_FIELD_NAME)
                .ifPresent(this::setTimeFieldName);
        descriptorProperties.getOptionalString(FORMAT_ATTRIBUTES_FIELD_NAME)
                .ifPresent(this::setAttributesFieldName);
        descriptorProperties.getOptionalString(FORMAT_METADATA_FIELD_NAME)
                .ifPresent(this::setMetadataFieldName);
        descriptorProperties.getOptionalBoolean(FORMAT_IGNORE_ERRORS)
                .ifPresent(this::setIgnoreErrors);
        descriptorProperties.getOptionalBoolean(FORMAT_INCLUDE_UPDATE_BEFORE)
                .ifPresent(this::setIncludeUpdateBefore);

        return (T) this;
    }
}
