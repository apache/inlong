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

package org.apache.inlong.sort.formats.inlongmsgtlogcsv;

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

import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DELIMITER;

/**
 * The deserializer for the records in InLongMsgTlogCsv format.
 */
public final class InLongMsgTlogCsvMixedFormatDeserializer
        extends
            AbstractInLongMsgMixedFormatDeserializer {

    private static final long serialVersionUID = 1L;

    /**
     * The charset of the text.
     */
    private final String charset;

    /**
     * The delimiter between fields.
     */
    @Nonnull
    private final Character delimiter;

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

    @Nonnull
    private Boolean isIncludeFirstSegment = false;

    public InLongMsgTlogCsvMixedFormatDeserializer(
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nonnull Boolean ignoreErrors) {
        this(charset, delimiter, escapeChar, quoteChar, false,
                InLongMsgUtils.getDefaultExceptionHandler(ignoreErrors));
    }

    public InLongMsgTlogCsvMixedFormatDeserializer(
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            @Nonnull Boolean isIncludeFirstSegment,
            @Nonnull FailureHandler failureHandler) {
        super(failureHandler);

        this.delimiter = delimiter;
        this.charset = charset;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.isIncludeFirstSegment = isIncludeFirstSegment;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return InLongMsgUtils.MIXED_ROW_TYPE;
    }

    @Override
    protected InLongMsgHead parseHead(String attr) throws Exception {
        return InLongMsgTlogCsvUtils.parseHead(attr);
    }

    @Override
    protected List<InLongMsgBody> parseBodyList(byte[] bytes) throws Exception {
        return Collections.singletonList(
                InLongMsgTlogCsvUtils.parseBody(bytes, charset, delimiter, escapeChar,
                        quoteChar, isIncludeFirstSegment));
    }

    @Override
    protected List<Row> convertRows(InLongMsgHead head, InLongMsgBody body) throws Exception {
        Row row = InLongMsgUtils.buildMixedRow(head, body, body.getStreamId());
        return Collections.singletonList(row);
    }

    /**
     * The builder for {@link InLongMsgTlogCsvMixedFormatDeserializer}.
     */
    public static class Builder extends InLongMsgTextMixedFormatDeserializerBuilder<Builder> {

        private Character delimiter;

        public Builder setDelimiter(char delimiter) {
            this.delimiter = delimiter;
            return this;
        }

        @Override
        public Builder configure(DescriptorProperties descriptorProperties) {
            super.configure(descriptorProperties);

            descriptorProperties.getOptionalCharacter(FORMAT_DELIMITER)
                    .ifPresent(this::setDelimiter);

            return this;
        }

        public InLongMsgTlogCsvMixedFormatDeserializer build() {
            return new InLongMsgTlogCsvMixedFormatDeserializer(
                    charset,
                    delimiter,
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
        InLongMsgTlogCsvMixedFormatDeserializer that = (InLongMsgTlogCsvMixedFormatDeserializer) o;
        return charset.equals(that.charset) &&
                delimiter.equals(that.delimiter) &&
                Objects.equals(escapeChar, that.escapeChar) &&
                Objects.equals(quoteChar, that.quoteChar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), charset, delimiter, escapeChar,
                quoteChar);
    }
}
