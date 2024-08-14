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

package org.apache.inlong.sort.formats.inlongmsgcsv;

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
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_LINE_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsgcsv.InLongMsgCsvUtils.DEFAULT_DELETE_HEAD_DELIMITER;
import static org.apache.inlong.sort.formats.inlongmsgcsv.InLongMsgCsvUtils.FORMAT_DELETE_HEAD_DELIMITER;

/**
 * The deserializer for the records in InLongMsgCsv format.
 */
public final class InLongMsgCsvMixedFormatDeserializer extends AbstractInLongMsgMixedFormatDeserializer {

    private static final long serialVersionUID = 1L;

    /**
     * The delimiter between fields.
     */
    @Nonnull
    private final Character delimiter;

    /**
     * The delimiter between lines.
     */
    @Nullable
    private final Character lineDelimiter;

    /**
     * The charset of the text.
     */
    @Nonnull
    private final String charset;

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

    /**
     * True if the head delimiter should be removed.
     */
    private final boolean deleteHeadDelimiter;

    public InLongMsgCsvMixedFormatDeserializer(
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character lineDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            boolean deleteHeadDelimiter,
            boolean ignoreErrors) {
        this(charset, delimiter, lineDelimiter, escapeChar, quoteChar, deleteHeadDelimiter,
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public InLongMsgCsvMixedFormatDeserializer(
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character lineDelimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            boolean deleteHeadDelimiter,
            @Nonnull FailureHandler failureHandler) {
        super(failureHandler);

        this.charset = charset;
        this.delimiter = delimiter;
        this.lineDelimiter = lineDelimiter;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.deleteHeadDelimiter = deleteHeadDelimiter;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return InLongMsgUtils.MIXED_ROW_TYPE;
    }

    @Override
    protected InLongMsgHead parseHead(String attr) {
        return InLongMsgCsvUtils.parseHead(attr);
    }

    @Override
    protected List<InLongMsgBody> parseBodyList(byte[] bytes) {
        return InLongMsgCsvUtils.parseBodyList(
                bytes,
                charset,
                delimiter,
                lineDelimiter,
                escapeChar,
                quoteChar,
                deleteHeadDelimiter);
    }

    @Override
    protected List<Row> convertRows(InLongMsgHead head, InLongMsgBody body) {
        Row row = InLongMsgUtils.buildMixedRow(head, body, head.getStreamId());
        return Collections.singletonList(row);
    }

    /**
     * The builder for {@link InLongMsgCsvMixedFormatDeserializer}.
     */
    public static class Builder extends InLongMsgTextMixedFormatDeserializerBuilder<Builder> {

        private Character delimiter = DEFAULT_DELIMITER;
        private Character lineDelimiter = DEFAULT_LINE_DELIMITER;
        private Boolean deleteHeadDelimiter = DEFAULT_DELETE_HEAD_DELIMITER;

        public Builder setDelimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        public Builder setLineDelimiter(char lineDelimiter) {
            this.lineDelimiter = lineDelimiter;
            return this;
        }

        public Builder setDeleteHeadDelimiter(boolean deleteHeadDelimiter) {
            this.deleteHeadDelimiter = deleteHeadDelimiter;
            return this;
        }

        @Override
        public Builder configure(DescriptorProperties descriptorProperties) {
            super.configure(descriptorProperties);

            descriptorProperties.getOptionalCharacter(FORMAT_DELIMITER)
                    .ifPresent(this::setDelimiter);
            descriptorProperties.getOptionalCharacter(FORMAT_LINE_DELIMITER)
                    .ifPresent(this::setLineDelimiter);
            descriptorProperties.getOptionalBoolean(FORMAT_DELETE_HEAD_DELIMITER)
                    .ifPresent(this::setDeleteHeadDelimiter);

            return this;
        }

        public InLongMsgCsvMixedFormatDeserializer build() {
            return new InLongMsgCsvMixedFormatDeserializer(
                    charset,
                    delimiter,
                    lineDelimiter,
                    escapeChar,
                    quoteChar,
                    deleteHeadDelimiter,
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

        InLongMsgCsvMixedFormatDeserializer that = (InLongMsgCsvMixedFormatDeserializer) object;
        return charset.equals(that.charset) &&
                delimiter.equals(that.delimiter) &&
                Objects.equals(lineDelimiter, that.lineDelimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar) &&
                deleteHeadDelimiter == that.deleteHeadDelimiter;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), delimiter, lineDelimiter, charset, escapeChar,
                quoteChar, deleteHeadDelimiter);
    }
}