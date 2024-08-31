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

package org.apache.inlong.sort.formats.inlongmsg.row;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;

import org.apache.flink.table.descriptors.DescriptorProperties;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.DEFAULT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_TIME_FIELD_NAME;

/**
 * The builder for {@link AbstractInLongMsgMixedFormatConverter}s.
 */
public abstract class InLongMsgMixedFormatConverterBuilder<T extends InLongMsgMixedFormatConverterBuilder> {

    protected final RowFormatInfo rowFormatInfo;

    protected String timeFieldName = DEFAULT_TIME_FIELD_NAME;
    protected String attributesFieldName = DEFAULT_ATTRIBUTES_FIELD_NAME;
    protected String nullLiteral = DEFAULT_NULL_LITERAL;
    protected boolean ignoreErrors = DEFAULT_IGNORE_ERRORS;

    protected InLongMsgMixedFormatConverterBuilder(RowFormatInfo rowFormatInfo) {
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
    public T setNullLiteral(String nullLiteral) {
        this.nullLiteral = nullLiteral;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setIgnoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T configure(DescriptorProperties descriptorProperties) {
        descriptorProperties.getOptionalString(FORMAT_TIME_FIELD_NAME)
                .ifPresent(this::setTimeFieldName);
        descriptorProperties.getOptionalString(FORMAT_ATTRIBUTES_FIELD_NAME)
                .ifPresent(this::setAttributesFieldName);
        descriptorProperties.getOptionalString(FORMAT_NULL_LITERAL)
                .ifPresent(this::setNullLiteral);
        descriptorProperties.getOptionalBoolean(FORMAT_IGNORE_ERRORS)
                .ifPresent(this::setIgnoreErrors);

        return (T) this;
    }
}
