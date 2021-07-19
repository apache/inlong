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

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.FORMAT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.getDataFormatInfo;
import static org.apache.inlong.sort.formats.tdmsg.TDMsgUtils.validateFieldNames;
import static org.apache.inlong.sort.formats.tdmsgcsv.TDMsgCsvUtils.FORMAT_DELETE_HEAD_DELIMITER;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;
import org.apache.inlong.sort.formats.base.TableFormatConstants;
import org.apache.inlong.sort.formats.base.TableFormatDeserializer;
import org.apache.inlong.sort.formats.base.TableFormatDeserializerFactory;
import org.apache.inlong.sort.formats.common.RowFormatInfo;
import org.apache.inlong.sort.formats.tdmsg.TDMsgMixedFormatFactory;
import org.apache.inlong.sort.formats.tdmsg.TDMsgMixedValidator;
import org.apache.inlong.sort.formats.tdmsg.TDMsgUtils;
import org.apache.inlong.sort.formats.tdmsg.TDMsgValidator;

/**
 * Table format factory for providing configured instances of TDMsgCsv-to-row
 * serializer and deserializer.
 */
public final class TDMsgCsvFormatFactory
        extends TableFormatFactoryBase<Row>
        implements TableFormatDeserializerFactory, TDMsgMixedFormatFactory {

    public TDMsgCsvFormatFactory() {
        super(TDMsgCsv.FORMAT_TYPE_VALUE, 1, true);
    }

    @Override
    public List<String> supportedFormatProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(FORMAT_CHARSET);
        properties.add(FORMAT_DELIMITER);
        properties.add(FORMAT_ESCAPE_CHARACTER);
        properties.add(FORMAT_QUOTE_CHARACTER);
        properties.add(FORMAT_NULL_LITERAL);
        properties.add(FORMAT_DELETE_HEAD_DELIMITER);
        properties.add(TableFormatConstants.FORMAT_SCHEMA);
        properties.add(FORMAT_TIME_FIELD_NAME);
        properties.add(FORMAT_ATTRIBUTES_FIELD_NAME);
        properties.add(FORMAT_IGNORE_ERRORS);
        return properties;
    }

    @Override
    public TableFormatDeserializer createFormatDeserializer(
            Map<String, String> properties
    ) {
        final DescriptorProperties descriptorProperties =
                new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        final TDMsgValidator validator = new TDMsgValidator();
        validator.validate(descriptorProperties);

        RowFormatInfo rowFormatInfo = getDataFormatInfo(descriptorProperties);

        String timeFieldName =
                descriptorProperties
                        .getOptionalString(FORMAT_TIME_FIELD_NAME)
                        .orElse(TDMsgUtils.DEFAULT_TIME_FIELD_NAME);
        String attributesFieldName =
                descriptorProperties
                        .getOptionalString(FORMAT_ATTRIBUTES_FIELD_NAME)
                        .orElse(TDMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME);

        validateFieldNames(timeFieldName, attributesFieldName, rowFormatInfo);

        String charset =
                descriptorProperties
                        .getOptionalString(FORMAT_CHARSET)
                        .orElse(DEFAULT_CHARSET);
        Character delimiter =
                descriptorProperties
                        .getOptionalCharacter(FORMAT_DELIMITER)
                        .orElse(DEFAULT_DELIMITER);
        Character escapeCharacter =
                descriptorProperties
                        .getOptionalCharacter(FORMAT_ESCAPE_CHARACTER)
                        .orElse(null);
        Character quoteCharacter =
                descriptorProperties
                        .getOptionalCharacter(FORMAT_QUOTE_CHARACTER)
                        .orElse(null);
        String nullLiteral =
                descriptorProperties
                        .getOptionalString(FORMAT_NULL_LITERAL)
                        .orElse(null);
        Boolean deleteHeadDelimiter =
                descriptorProperties
                        .getOptionalBoolean(FORMAT_DELETE_HEAD_DELIMITER)
                        .orElse(TDMsgCsvUtils.DEFAULT_DELETE_HEAD_DELIMITER);
        boolean ignoreErrors =
                descriptorProperties
                        .getOptionalBoolean(FORMAT_IGNORE_ERRORS)
                        .orElse(DEFAULT_IGNORE_ERRORS);

        return new TDMsgCsvFormatDeserializer(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                charset,
                delimiter,
                escapeCharacter,
                quoteCharacter,
                nullLiteral,
                deleteHeadDelimiter,
                ignoreErrors
        );
    }

    @Override
    public TDMsgCsvMixedFormatDeserializer createMixedFormatDeserializer(
            Map<String, String> properties
    ) {
        final DescriptorProperties descriptorProperties =
                new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        final TDMsgMixedValidator validator = new TDMsgMixedValidator();
        validator.validate(descriptorProperties);

        String charset =
                descriptorProperties
                        .getOptionalString(FORMAT_CHARSET)
                        .orElse(DEFAULT_CHARSET);
        Character delimiter =
                descriptorProperties
                        .getOptionalCharacter(FORMAT_DELIMITER)
                        .orElse(DEFAULT_DELIMITER);
        Character escapeCharacter =
                descriptorProperties
                        .getOptionalCharacter(FORMAT_ESCAPE_CHARACTER)
                        .orElse(null);
        Character quoteCharacter =
                descriptorProperties
                        .getOptionalCharacter(FORMAT_QUOTE_CHARACTER)
                        .orElse(null);
        Boolean deleteHeadDelimiter =
                descriptorProperties
                        .getOptionalBoolean(FORMAT_DELETE_HEAD_DELIMITER)
                        .orElse(TDMsgCsvUtils.DEFAULT_DELETE_HEAD_DELIMITER);
        boolean ignoreErrors =
                descriptorProperties
                        .getOptionalBoolean(FORMAT_IGNORE_ERRORS)
                        .orElse(DEFAULT_IGNORE_ERRORS);

        return new TDMsgCsvMixedFormatDeserializer(
                charset,
                delimiter,
                escapeCharacter,
                quoteCharacter,
                deleteHeadDelimiter,
                ignoreErrors
        );
    }

    @Override
    public TDMsgCsvMixedFormatConverter createMixedFormatConverter(
            Map<String, String> properties
    ) {
        final DescriptorProperties descriptorProperties =
                new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        RowFormatInfo rowFormatInfo = getDataFormatInfo(descriptorProperties);

        String timeFieldName =
                descriptorProperties
                        .getOptionalString(FORMAT_TIME_FIELD_NAME)
                        .orElse(TDMsgUtils.DEFAULT_TIME_FIELD_NAME);
        String attributesFieldName =
                descriptorProperties
                        .getOptionalString(FORMAT_ATTRIBUTES_FIELD_NAME)
                        .orElse(TDMsgUtils.DEFAULT_ATTRIBUTES_FIELD_NAME);

        validateFieldNames(timeFieldName, attributesFieldName, rowFormatInfo);

        String nullLiteral =
                descriptorProperties
                        .getOptionalString(FORMAT_NULL_LITERAL)
                        .orElse(null);
        boolean ignoreErrors =
                descriptorProperties
                        .getOptionalBoolean(FORMAT_IGNORE_ERRORS)
                        .orElse(DEFAULT_IGNORE_ERRORS);

        return new TDMsgCsvMixedFormatConverter(
                rowFormatInfo,
                timeFieldName,
                attributesFieldName,
                nullLiteral,
                ignoreErrors
        );
    }
}
