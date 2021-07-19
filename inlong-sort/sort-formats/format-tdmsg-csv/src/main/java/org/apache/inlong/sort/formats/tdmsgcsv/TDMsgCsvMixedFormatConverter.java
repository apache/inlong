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

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.tdmsg.TDMsgMixedFormatConverter;
import org.apache.inlong.sort.formats.tdmsg.TDMsgUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converter used to deserialize a mixed row in tdmsg-csv format.
 */
public class TDMsgCsvMixedFormatConverter implements TDMsgMixedFormatConverter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(TDMsgCsvMixedFormatConverter.class);

    /**
     * The schema of the rows.
     */
    @Nonnull
    private final RowFormatInfo rowFormatInfo;

    /**
     * The name of the time field.
     */
    @Nonnull
    private final String timeFieldName;

    /**
     * The name of the attributes field.
     */
    @Nonnull
    private final String attributesFieldName;

    /**
     * The literal representing null values.
     */
    private final String nullLiteral;

    /**
     * True if ignore errors in the deserialization.
     */
    private final boolean ignoreErrors;

    public TDMsgCsvMixedFormatConverter(
            @Nonnull RowFormatInfo rowFormatInfo,
            @Nonnull String timeFieldName,
            @Nonnull String attributesFieldName,
            String nullLiteral,
            boolean ignoreErrors
    ) {
        this.rowFormatInfo = rowFormatInfo;
        this.timeFieldName = timeFieldName;
        this.attributesFieldName = attributesFieldName;
        this.nullLiteral = nullLiteral;
        this.ignoreErrors = ignoreErrors;
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TDMsgUtils.buildRowType(timeFieldName, attributesFieldName, rowFormatInfo);
    }

    @Override
    public void flatMap(Row mixedRow, Collector<Row> collector) {

        Row row;

        try {
            Timestamp time = TDMsgUtils.getTimeFromMixedRow(mixedRow);
            Map<String, String> attributes = TDMsgUtils.getAttributesFromMixedRow(mixedRow);
            List<String> predefinedFields = TDMsgUtils.getPredefinedFieldsFromMixedRow(mixedRow);
            List<String> fields = TDMsgUtils.getFieldsFromMixedRow(mixedRow);

            row = TDMsgCsvUtils.buildRow(rowFormatInfo, nullLiteral, time, attributes,
                    predefinedFields, fields);
        } catch (Exception e) {
            if (ignoreErrors) {
                LOG.warn("Cannot properly convert the mixed row {} to row.",
                        mixedRow, e);
                return;
            } else {
                throw e;
            }
        }

        collector.collect(row);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TDMsgCsvMixedFormatConverter that = (TDMsgCsvMixedFormatConverter) o;
        return ignoreErrors == that.ignoreErrors
                       && rowFormatInfo.equals(that.rowFormatInfo)
                       && timeFieldName.equals(that.timeFieldName)
                       && attributesFieldName.equals(that.attributesFieldName)
                       && Objects.equals(nullLiteral, that.nullLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rowFormatInfo, timeFieldName, attributesFieldName, nullLiteral,
                ignoreErrors);
    }
}
