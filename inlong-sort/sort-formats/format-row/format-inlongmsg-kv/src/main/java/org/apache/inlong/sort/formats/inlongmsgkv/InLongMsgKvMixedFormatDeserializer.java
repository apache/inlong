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

package org.apache.inlong.sort.formats.inlongmsgkv;

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

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsgkv.InLongMsgKvUtils.DEFAULT_INLONGMSGKV_CHARSET;

/**
 * The deserializer for the records in InLongMsgKv format.
 */
public final class InLongMsgKvMixedFormatDeserializer
        extends
            AbstractInLongMsgMixedFormatDeserializer {

    private static final long serialVersionUID = 1L;

    /**
     * The charset of the text.
     */
    @Nonnull
    private final String charset;

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
     * The delimiter between lines.
     */
    @Nullable
    private final Character lineDelimiter;

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

    public InLongMsgKvMixedFormatDeserializer(
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character lineDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nonnull Boolean ignoreErrors) {
        this(
                charset,
                entryDelimiter,
                kvDelimiter,
                lineDelimiter,
                escapeChar,
                quoteChar,
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public InLongMsgKvMixedFormatDeserializer(
            @Nonnull String charset,
            @Nonnull Character entryDelimiter,
            @Nonnull Character kvDelimiter,
            @Nullable Character lineDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nonnull FailureHandler failureHandler) {
        super(failureHandler);

        this.entryDelimiter = entryDelimiter;
        this.kvDelimiter = kvDelimiter;
        this.lineDelimiter = lineDelimiter;
        this.charset = charset;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return InLongMsgUtils.MIXED_ROW_TYPE;
    }

    @Override
    protected InLongMsgHead parseHead(String attr) {
        return InLongMsgKvUtils.parseHead(attr);
    }

    @Override
    protected List<InLongMsgBody> parseBodyList(byte[] bytes) throws Exception {
        return InLongMsgKvUtils.parseBodyList(
                bytes,
                charset,
                entryDelimiter,
                kvDelimiter,
                lineDelimiter,
                escapeChar,
                quoteChar);
    }

    @Override
    protected List<Row> convertRows(InLongMsgHead head, InLongMsgBody body) {
        Row row = InLongMsgUtils.buildMixedRow(head, body, head.getStreamId());
        return Collections.singletonList(row);
    }

    /**
     * The builder for {@link InLongMsgKvMixedFormatDeserializer}.
     */
    public static class Builder extends InLongMsgTextMixedFormatDeserializerBuilder<Builder> {

        private Character entryDelimiter = DEFAULT_ENTRY_DELIMITER;
        private Character kvDelimiter = DEFAULT_KV_DELIMITER;
        private Character lineDelimiter = DEFAULT_LINE_DELIMITER;

        public Builder() {
            super();

            this.charset = DEFAULT_INLONGMSGKV_CHARSET;
        }

        public Builder setEntryDelimiter(Character entryDelimiter) {
            this.entryDelimiter = entryDelimiter;
            return this;
        }

        public Builder setKvDelimiter(Character kvDelimiter) {
            this.kvDelimiter = kvDelimiter;
            return this;
        }

        public Builder setLineDelimiter(Character lineDelimiter) {
            this.lineDelimiter = lineDelimiter;
            return this;
        }

        @Override
        public Builder configure(DescriptorProperties descriptorProperties) {
            super.configure(descriptorProperties);

            descriptorProperties.getOptionalCharacter(FORMAT_ENTRY_DELIMITER)
                    .ifPresent(this::setEntryDelimiter);
            descriptorProperties.getOptionalCharacter(FORMAT_KV_DELIMITER)
                    .ifPresent(this::setKvDelimiter);
            descriptorProperties.getOptionalCharacter(FORMAT_LINE_DELIMITER)
                    .ifPresent(this::setLineDelimiter);

            return this;
        }

        public InLongMsgKvMixedFormatDeserializer build() {
            return new InLongMsgKvMixedFormatDeserializer(
                    charset,
                    entryDelimiter,
                    kvDelimiter,
                    lineDelimiter,
                    escapeChar,
                    quoteChar,
                    ignoreErrors);
        }
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        if (!super.equals(object)) {
            return false;
        }

        InLongMsgKvMixedFormatDeserializer that = (InLongMsgKvMixedFormatDeserializer) object;
        return charset.equals(that.charset) &&
                entryDelimiter.equals(that.entryDelimiter) &&
                kvDelimiter.equals(that.kvDelimiter) &&
                Objects.equals(lineDelimiter, that.lineDelimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), charset, entryDelimiter, kvDelimiter, lineDelimiter,
                escapeChar, quoteChar);
    }
}
