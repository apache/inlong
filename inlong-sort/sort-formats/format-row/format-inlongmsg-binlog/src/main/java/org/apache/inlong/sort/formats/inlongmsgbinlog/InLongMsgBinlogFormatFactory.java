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
import org.apache.inlong.sort.formats.base.TableFormatDeserializer;
import org.apache.inlong.sort.formats.base.TableFormatDeserializerFactory;
import org.apache.inlong.sort.formats.inlongmsg.row.AbstractInLongMsgFormatDeserializer;
import org.apache.inlong.sort.formats.inlongmsg.row.AbstractInLongMsgMixedFormatConverter;
import org.apache.inlong.sort.formats.inlongmsg.row.AbstractInLongMsgMixedFormatDeserializer;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgMixedFormatDeserializerValidator;
import org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgMixedFormatFactory;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.DEFAULT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_SCHEMA;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.FORMAT_INCLUDE_UPDATE_BEFORE;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.FORMAT_METADATA_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.getDataRowFormatInfo;

/**
 * Table format factory for providing configured instances of InLongMsgBinlog-to-row
 * serializer and deserializer.
 */

public final class InLongMsgBinlogFormatFactory extends TableFormatFactoryBase<Row>
        implements
            TableFormatDeserializerFactory,
            InLongMsgMixedFormatFactory {

    public InLongMsgBinlogFormatFactory() {
        super(InLongMsgBinlog.FORMAT_TYPE_VALUE, 1, false);
    }

    @Override
    public List<String> supportedFormatProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(FORMAT_SCHEMA);
        properties.add(FORMAT_TIME_FIELD_NAME);
        properties.add(FORMAT_ATTRIBUTES_FIELD_NAME);
        properties.add(FORMAT_METADATA_FIELD_NAME);
        properties.add(FORMAT_IGNORE_ERRORS);
        properties.add(FORMAT_INCLUDE_UPDATE_BEFORE);

        return properties;
    }

    @Override
    public InLongMsgBinlogFormatDeserializer createFormatDeserializer(
            Map<String, String> properties) {
        final DescriptorProperties descriptorProperties =
                new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        final InLongMsgBinlogValidator validator = new InLongMsgBinlogValidator();
        validator.validate(descriptorProperties);

        RowFormatInfo dataRowFormatInfo = getDataRowFormatInfo(descriptorProperties);
        InLongMsgBinlogFormatDeserializer.Builder builder =
                new InLongMsgBinlogFormatDeserializer.Builder(dataRowFormatInfo);
        builder.configure(descriptorProperties);

        return builder.build();
    }

    @Override
    public TableFormatDeserializer createFormatDeserializer(TableFormatDeserializer.TableFormatContext context) {
        TableFormatDeserializer deserializer =
                createFormatDeserializer(context.getFormatProperties());
        deserializer.init(context);
        return deserializer;
    }

    @Override
    public InLongMsgBinlogMixedFormatDeserializer createMixedFormatDeserializer(
            Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        Boolean ignoreErrors =
                descriptorProperties
                        .getOptionalBoolean(FORMAT_IGNORE_ERRORS)
                        .orElse(DEFAULT_IGNORE_ERRORS);

        return new InLongMsgBinlogMixedFormatDeserializer(ignoreErrors);
    }

    @Override
    public AbstractInLongMsgMixedFormatConverter createMixedFormatConverter(
            AbstractInLongMsgMixedFormatConverter.TableFormatContext context) {
        return null;
    }

    @Override
    public AbstractInLongMsgMixedFormatDeserializer createMixedFormatDeserializer(
            AbstractInLongMsgFormatDeserializer.TableFormatContext context) {
        return null;
    }

    @Override
    public InLongMsgBinlogMixedFormatConverter createMixedFormatConverter(
            Map<String, String> properties) {
        DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        final InLongMsgMixedFormatDeserializerValidator validator =
                new InLongMsgMixedFormatDeserializerValidator();
        validator.validate(descriptorProperties);

        RowFormatInfo dataRowFormatInfo = getDataRowFormatInfo(descriptorProperties);
        InLongMsgBinlogMixedFormatConverter.Builder builder =
                new InLongMsgBinlogMixedFormatConverter.Builder(dataRowFormatInfo);
        builder.configure(descriptorProperties);

        return builder.build();
    }
}
