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

package org.apache.inlong.sort.formats.base;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;

import org.apache.flink.table.descriptors.DescriptorProperties;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_QUOTE_CHARACTER;

/**
 * The base class to build the serializers and deserializers for text formats.
 */
public abstract class TextFormatBuilder<T extends TextFormatBuilder> {

    protected final RowFormatInfo rowFormatInfo;

    protected String charset = DEFAULT_CHARSET;
    protected Character escapeChar = DEFAULT_ESCAPE_CHARACTER;
    protected Character quoteChar = DEFAULT_QUOTE_CHARACTER;
    protected String nullLiteral = DEFAULT_NULL_LITERAL;
    protected boolean ignoreErrors = DEFAULT_IGNORE_ERRORS;
    protected FailureHandler failureHandler;

    protected TextFormatBuilder(RowFormatInfo rowFormatInfo) {
        this.rowFormatInfo = rowFormatInfo;
    }

    @SuppressWarnings("unchecked")
    public T setCharset(String charset) {
        this.charset = charset;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setEscapeCharacter(char escapeChar) {
        this.escapeChar = escapeChar;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setQuoteCharacter(char quoteChar) {
        this.quoteChar = quoteChar;
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

    public T setFailureHandler(FailureHandler failureHandler) {
        this.failureHandler = failureHandler;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T configure(DescriptorProperties descriptorProperties) {

        descriptorProperties.getOptionalString(FORMAT_CHARSET)
                .ifPresent(this::setCharset);

        descriptorProperties.getOptionalCharacter(FORMAT_ESCAPE_CHARACTER)
                .ifPresent(this::setEscapeCharacter);

        descriptorProperties.getOptionalCharacter(FORMAT_QUOTE_CHARACTER)
                .ifPresent(this::setQuoteCharacter);

        descriptorProperties.getOptionalString(FORMAT_NULL_LITERAL)
                .ifPresent(this::setNullLiteral);

        descriptorProperties.getOptionalBoolean(FORMAT_IGNORE_ERRORS)
                .ifPresent(this::setIgnoreErrors);

        return (T) this;
    }
}
