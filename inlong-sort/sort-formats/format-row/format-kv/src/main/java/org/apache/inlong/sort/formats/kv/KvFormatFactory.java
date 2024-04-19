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

package org.apache.inlong.sort.formats.kv;

import org.apache.inlong.common.pojo.sort.dataflow.field.format.BasicFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.RowFormatInfo;
import org.apache.inlong.sort.formats.base.DefaultTableFormatDeserializer;
import org.apache.inlong.sort.formats.base.DefaultTableFormatSerializer;
import org.apache.inlong.sort.formats.base.ProjectedDeserializationSchemaFactory;
import org.apache.inlong.sort.formats.base.ProjectedSerializationSchemaFactory;
import org.apache.inlong.sort.formats.base.TableFormatDeserializer;
import org.apache.inlong.sort.formats.base.TableFormatDeserializerFactory;
import org.apache.inlong.sort.formats.base.TableFormatSerializer;
import org.apache.inlong.sort.formats.base.TableFormatSerializerFactory;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_CHARSET;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ENTRY_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_ESCAPE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_KV_DELIMITER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_NULL_LITERAL;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_QUOTE_CHARACTER;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_SCHEMA;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.getRowFormatInfo;
import static org.apache.inlong.sort.formats.base.TableFormatUtils.projectRowFormatInfo;
import static org.apache.inlong.sort.formats.kv.Kv.FORMAT_TYPE_VALUE;

/**
 * Table format factory for kv formats.
 */
public final class KvFormatFactory
        extends
            TableFormatFactoryBase<Row>
        implements
            DeserializationSchemaFactory<Row>,
            SerializationSchemaFactory<Row>,
            ProjectedDeserializationSchemaFactory,
            ProjectedSerializationSchemaFactory,
            TableFormatSerializerFactory,
            TableFormatDeserializerFactory {

    public KvFormatFactory() {
        super(FORMAT_TYPE_VALUE, 1, true);
    }

    @Override
    public List<String> supportedFormatProperties() {
        final List<String> properties = new ArrayList<>();
        properties.add(FORMAT_ENTRY_DELIMITER);
        properties.add(FORMAT_KV_DELIMITER);
        properties.add(FORMAT_ESCAPE_CHARACTER);
        properties.add(FORMAT_QUOTE_CHARACTER);
        properties.add(FORMAT_NULL_LITERAL);
        properties.add(FORMAT_CHARSET);
        properties.add(FORMAT_IGNORE_ERRORS);
        properties.add(FORMAT_SCHEMA);
        return properties;
    }

    @Override
    public KvDeserializationSchema createDeserializationSchema(
            Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        RowFormatInfo rowFormatInfo = getValidatedRowFormatInfo(descriptorProperties);

        return buildDeserializationSchema(rowFormatInfo, descriptorProperties);
    }

    @Override
    public KvSerializationSchema createSerializationSchema(
            Map<String, String> properties) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        RowFormatInfo rowFormatInfo = getValidatedRowFormatInfo(descriptorProperties);

        return buildSerializationSchema(rowFormatInfo, descriptorProperties);
    }

    @Override
    public DeserializationSchema<Row> createProjectedDeserializationSchema(
            Map<String, String> properties,
            int[] fields) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        RowFormatInfo rowFormatInfo = getValidatedRowFormatInfo(descriptorProperties);

        final RowFormatInfo projectedRowFormatInfo =
                projectRowFormatInfo(rowFormatInfo, fields);

        return buildDeserializationSchema(projectedRowFormatInfo, descriptorProperties);
    }

    @Override
    public SerializationSchema<Row> createProjectedSerializationSchema(
            Map<String, String> properties,
            int[] fields) {
        DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        RowFormatInfo rowFormatInfo = getValidatedRowFormatInfo(descriptorProperties);

        final RowFormatInfo projectedRowFormatInfo =
                projectRowFormatInfo(rowFormatInfo, fields);

        return buildSerializationSchema(projectedRowFormatInfo, descriptorProperties);
    }

    @Override
    public TableFormatDeserializer createFormatDeserializer(Map<String, String> properties) {
        DeserializationSchema<Row> deserializationSchema = createDeserializationSchema(properties);
        return new DefaultTableFormatDeserializer(deserializationSchema);
    }

    @Override
    public TableFormatDeserializer createFormatDeserializer(TableFormatDeserializer.TableFormatContext context) {
        TableFormatDeserializer deserializer =
                createFormatDeserializer(context.getFormatProperties());
        deserializer.init(context);
        return deserializer;
    }

    @Override
    public TableFormatSerializer createFormatSerializer(Map<String, String> properties) {
        SerializationSchema<Row> serializationSchema = createSerializationSchema(properties);
        return new DefaultTableFormatSerializer(serializationSchema);
    }

    @Override
    public TableFormatSerializer createFormatSerializer(
            TableFormatSerializer.TableFormatContext context) {
        TableFormatSerializer serializer =
                createFormatSerializer(context.getFormatProperties());
        serializer.init(context);
        return serializer;
    }

    private static DescriptorProperties getValidatedProperties(
            Map<String, String> properties) {
        DescriptorProperties descriptorProperties =
                new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);

        KvValidator validator = new KvValidator();
        validator.validate(descriptorProperties);

        return descriptorProperties;
    }

    private static RowFormatInfo getValidatedRowFormatInfo(
            DescriptorProperties descriptorProperties) {
        RowFormatInfo rowFormatInfo = getRowFormatInfo(descriptorProperties);

        String[] fieldNames = rowFormatInfo.getFieldNames();
        FormatInfo[] fieldFormatInfos = rowFormatInfo.getFieldFormatInfos();
        for (int i = 0; i < fieldNames.length; ++i) {
            String fieldName = fieldNames[i];
            FormatInfo fieldFormatInfo = fieldFormatInfos[i];

            if (!(fieldFormatInfo instanceof BasicFormatInfo)) {
                throw new ValidationException("The format for field " + fieldName + " is " +
                        fieldFormatInfo.getClass().getSimpleName() + ". Only basic formats are " +
                        "supported in kv.");
            }
        }

        return rowFormatInfo;
    }

    private static KvDeserializationSchema buildDeserializationSchema(
            RowFormatInfo rowFormatInfo,
            DescriptorProperties descriptorProperties) {
        KvDeserializationSchema.Builder builder =
                new KvDeserializationSchema.Builder(rowFormatInfo);

        builder.configure(descriptorProperties);

        return builder.build();
    }

    private static KvSerializationSchema buildSerializationSchema(
            RowFormatInfo rowFormatInfo,
            DescriptorProperties descriptorProperties

    ) {
        KvSerializationSchema.Builder builder =
                new KvSerializationSchema.Builder(rowFormatInfo);

        builder.configure(descriptorProperties);

        return builder.build();
    }
}
