/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.formats.tdmsgcsv;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.base.TableFormatConstants;
import org.apache.inlong.sort.formats.tdmsg.AbstractTDMsgFormatDeserializer;
import org.apache.inlong.sort.formats.tdmsg.AbstractTDMsgMixedFormatDeserializer;
import org.apache.inlong.sort.formats.tdmsg.TDMsgBody;
import org.apache.inlong.sort.formats.tdmsg.TDMsgHead;
import org.apache.inlong.sort.formats.tdmsg.TDMsgUtils;

/**
 * The deserializer for the records in TDMsgCsv format.
 */
public final class TDMsgCsvMixedFormatDeserializer extends AbstractTDMsgMixedFormatDeserializer {

    private static final long serialVersionUID = 1L;

    /**
     * The delimiter between fields.
     */
    @Nonnull
    private final Character delimiter;

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

    public TDMsgCsvMixedFormatDeserializer(
            @Nonnull String charset,
            @Nonnull Character delimiter,
            @Nullable Character escapeChar,
            @Nullable Character quoteChar,
            boolean deleteHeadDelimiter,
            boolean ignoreErrors
    ) {
        super(ignoreErrors);

        this.delimiter = delimiter;
        this.charset = charset;
        this.escapeChar = escapeChar;
        this.quoteChar = quoteChar;
        this.deleteHeadDelimiter = deleteHeadDelimiter;
    }

    public TDMsgCsvMixedFormatDeserializer() {
        this(
                TableFormatConstants.DEFAULT_CHARSET,
                TableFormatConstants.DEFAULT_DELIMITER,
                null,
                null,
                TDMsgCsvUtils.DEFAULT_DELETE_HEAD_DELIMITER,
                TableFormatConstants.DEFAULT_IGNORE_ERRORS
        );
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TDMsgUtils.MIXED_ROW_TYPE;
    }

    @Override
    protected TDMsgHead parseHead(String attr) {
        return TDMsgCsvUtils.parseHead(attr);
    }

    @Override
    protected TDMsgBody parseBody(byte[] bytes) {
        return TDMsgCsvUtils.parseBody(
                bytes,
                charset,
                delimiter,
                escapeChar,
                quoteChar,
                deleteHeadDelimiter
        );
    }

    @Override
    protected Row convertRow(TDMsgHead head, TDMsgBody body) {
        return TDMsgUtils.buildMixedRow(head, body, head.getTid());
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

        TDMsgCsvMixedFormatDeserializer that = (TDMsgCsvMixedFormatDeserializer) o;
        return deleteHeadDelimiter == that.deleteHeadDelimiter
                       && charset.equals(that.charset)
                       && delimiter.equals(that.delimiter)
                       && Objects.equals(escapeChar, that.escapeChar)
                       && Objects.equals(quoteChar, that.quoteChar);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), charset, delimiter, escapeChar,
                quoteChar, deleteHeadDelimiter);
    }
}
