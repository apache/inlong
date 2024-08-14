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

package org.apache.inlong.sort.formats.inlongmsgtlogkv;

import org.apache.inlong.sort.formats.inlongmsg.FailureHandler;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgBody;
import org.apache.inlong.sort.formats.inlongmsg.InLongMsgHead;
import org.apache.inlong.sort.formats.inlongmsg.row.AbstractInLongMsgMixedFormatDeserializer;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgTextMixedFormatDeserializerBuilder;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.types.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsgtlogkv.InLongMsgTlogKvUtils.DEFAULT_INLONGMSG_TLOGKV_CHARSET;

/**
 * The deserializer for the records in InLongMsgTlogKV format.
 */
public final class InLongMsgTlogKvMixedFormatDeserializer
        extends
            AbstractInLongMsgMixedFormatDeserializer {

    private static final long serialVersionUID = 1L;

    /**
     * The charset of the text.
     */
    @Nonnull
    private final String charset;

    /**
     * The delimiter between fields.
     */
    @Nonnull
    private final Character delimiter;

    /**
     * The delimiter between entries.
     */
    @Nonnull
    private final Character entryDelimiter;

    /**
     * The delimiter between key and value.
     */
    @Nonnull
    private final Character kvDelimiter;

    /**
     * Escape character. Null if escaping is disabled.
     */
    @Nullable
    private final Character escapeChar;

    /**
     * Quote character. Null if quoting is disabled.
     */
    @Nullable
    private final Character quoteChar;

    public InLongMsgTlogKvMixedFormatDeserializer(
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nonnull Boolean ignoreErrors) {
        this(charset, delimiter, entryDelimiter, kvDelimiter, escapeChar, quoteChar,
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public InLongMsgTlogKvMixedFormatDeserializer(
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nonnull FailureHandler failureHandler) {
        super(failureHandler);

        this.charset = charset;
        this.delimiter = delimiter;
        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return InLongMsgUtils.MIXED_ROW_TYPE;
    }

    @Override
    protected InLongMsgHead parseHead(String attr) throws Exception {
        return InLongMsgTlogKvUtils.parseHead(attr);
    }

    @Override
    protected List<InLongMsgBody> parseBodyList(byte[] bytes) throws Exception {
        return Collections.singletonList(
                InLongMsgTlogKvUtils.parseBody(
                        bytes,
                        charset,
                        delimiter,
                        entryDelimiter,
                        kvDelimiter,
                        escapeChar,
                        quoteChar));
    }

    @Override
    protected List<Row> convertRows(InLongMsgHead head, InLongMsgBody body) throws Exception {
        Row row = InLongMsgUtils.buildMixedRow(head, body, body.getStreamId());
        return Collections.singletonList(row);
    }

    /**
     * The builder for {@link InLongMsgTlogKvMixedFormatDeserializer}.
     */
    public static class Builder extends InLongMsgTextMixedFormatDeserializerBuilder<Builder> {

        private Character delimiter = DEFAULT_DELIMITER;
        private Character entryDelimiter = DEFAULT_ENTRY_DELIMITER;
        private Character kvDelimiter = DEFAULT_KV_DELIMITER;

        public Builder() {
            super();
            this.charset = DEFAULT_INLONGMSG_TLOGKV_CHARSET;
        }

        public Builder setDelimiter(Character delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder setEntryDelimiter(Character entryDelimiter) {
            this.entryDelimiter = entryDelimiter;
            return this;
        }

        public Builder setKvDelimiter(Character kvDelimiter) {
            this.kvDelimiter = kvDelimiter;
            return this;
        }

        @Override
        public Builder configure(DescriptorProperties descriptorProperties) {
            super.configure(descriptorProperties);

            descriptorProperties.getOptionalCharacter(FORMAT_DELIMITER)
                    .ifPresent(this::setDelimiter);
            descriptorProperties.getOptionalCharacter(FORMAT_ENTRY_DELIMITER)
                    .ifPresent(this::setEntryDelimiter);
            descriptorProperties.getOptionalCharacter(FORMAT_KV_DELIMITER)
                    .ifPresent(this::setKvDelimiter);

            return this;
        }

        public InLongMsgTlogKvMixedFormatDeserializer build() {
            return new InLongMsgTlogKvMixedFormatDeserializer(
                    charset,
                    delimiter,
                    entryDelimiter,
                    kvDelimiter,
                    escapeChar,
                    quoteChar,
                    ignoreErrors);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        InLongMsgTlogKvMixedFormatDeserializer that = (InLongMsgTlogKvMixedFormatDeserializer) o;
        return delimiter.equals(that.delimiter) &&
                entryDelimiter.equals(that.entryDelimiter) &&
                kvDelimiter.equals(that.kvDelimiter) &&
                charset.equals(that.charset) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), delimiter, entryDelimiter,
                kvDelimiter, charset, escapeChar, quoteChar);
    }
}
