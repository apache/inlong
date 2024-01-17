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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.types.Row;

import java.util.Map;

public class TableFormatForRowUtils extends TableFormatUtils {

    /**
     * Returns the {@link DeserializationSchema} described by the given
     * properties.
     *
     * @param properties The properties describing the deserializer.
     * @param fields The fields to project.
     * @param classLoader The class loader for the deserializer.
     * @param <T> The type of the data.
     * @return The {@link DeserializationSchema} described by the properties.
     */
    public static <T> DeserializationSchema<Row> getProjectedDeserializationSchema(
            final Map<String, String> properties,
            final int[] fields,
            final ClassLoader classLoader) {
        final ProjectedDeserializationSchemaFactory deserializationSchemaFactory =
                TableFactoryService.find(
                        ProjectedDeserializationSchemaFactory.class,
                        properties,
                        classLoader);

        return deserializationSchemaFactory
                .createProjectedDeserializationSchema(properties, fields);
    }

    /**
     * Returns the {@link SerializationSchema} described by the given
     * properties.
     *
     * @param properties The properties describing the serializer.
     * @param fields The fields to project.
     * @param classLoader The class loader for the serializer.
     * @return The {@link SerializationSchema} described by the properties.
     */
    public static SerializationSchema<Row> getProjectedSerializationSchema(
            final Map<String, String> properties,
            final int[] fields,
            final ClassLoader classLoader) {
        final ProjectedSerializationSchemaFactory serializationSchemaFactory =
                TableFactoryService.find(
                        ProjectedSerializationSchemaFactory.class,
                        properties,
                        classLoader);

        return serializationSchemaFactory
                .createProjectedSerializationSchema(properties, fields);
    }

    /**
     * Returns the {@link TableFormatSerializer} described by the given
     * properties.
     *
     * @param properties The properties describing the serializer.
     * @param classLoader The class loader for the serializer.
     * @return The {@link TableFormatSerializer} described by the properties.
     */
    public static TableFormatSerializer getTableFormatSerializer(
            final Map<String, String> properties,
            final ClassLoader classLoader) {
        final TableFormatSerializerFactory tableFormatSerializerFactory =
                TableFactoryService.find(
                        TableFormatSerializerFactory.class,
                        properties,
                        classLoader);

        return tableFormatSerializerFactory
                .createFormatSerializer(properties);
    }

    /**
     * Returns the {@link TableFormatDeserializer} described by the
     * given properties.
     *
     * @param properties The properties describing the deserializer.
     * @param classLoader The class loader for the deserializer.
     * @return The {@link TableFormatDeserializer} described by the properties.
     */
    public static TableFormatDeserializer getTableFormatDeserializer(
            final Map<String, String> properties,
            final ClassLoader classLoader) {
        final TableFormatDeserializerFactory tableFormatDeserializerFactory =
                TableFactoryService.find(
                        TableFormatDeserializerFactory.class,
                        properties,
                        classLoader);

        return tableFormatDeserializerFactory
                .createFormatDeserializer(properties);
    }
}
