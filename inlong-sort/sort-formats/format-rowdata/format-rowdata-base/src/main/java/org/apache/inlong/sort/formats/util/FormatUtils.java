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

package org.apache.inlong.sort.formats.util;

import org.apache.inlong.sort.formats.metrics.FormatMetricGroup;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The utilities for formats.
 */
public class FormatUtils {

    /**
     * Check whether there are duplicated field names. Throws an {@link IllegalArgumentException}
     * with the duplicated field names if any exist.
     *
     * @param fieldNames the field names to be checked.
     */
    public static void checkDuplicates(String[] fieldNames) {
        Set<String> items = new HashSet<>(fieldNames.length);
        Set<String> duplicatedFieldNames =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !items.add(fieldName))
                        .collect(Collectors.toSet());

        if (!duplicatedFieldNames.isEmpty()) {
            throw new IllegalArgumentException(String.format("There exist duplicated " +
                    "field names: %s", duplicatedFieldNames));
        }
    }

    public static <T> void initializeDeserializationSchema(
            DeserializationSchema<T> deserializationSchema,
            RuntimeContext runtimeContext,
            Map<String, String> tags) throws Exception {
        FormatMetricGroup formatMetricGroup = new FormatMetricGroup(
                runtimeContext.getMetricGroup(),
                FormatMetricGroup.FORMAT_GROUP_NAME,
                true,
                tags);

        deserializationSchema.open(
                RuntimeContextInitializationContextAdapters.deserializationAdapter(runtimeContext,
                        metricGroup -> formatMetricGroup));
    }

    public static <T> void initializeDeserializationSchema(
            Collection<DeserializationSchema<T>> deserializationSchemas,
            RuntimeContext runtimeContext,
            Map<String, String> tags) throws Exception {
        FormatMetricGroup formatMetricGroup = new FormatMetricGroup(
                runtimeContext.getMetricGroup(),
                FormatMetricGroup.FORMAT_GROUP_NAME,
                true,
                tags);
        for (DeserializationSchema<T> deserializationSchema : deserializationSchemas) {
            deserializationSchema.open(RuntimeContextInitializationContextAdapters
                    .deserializationAdapter(runtimeContext, metricGroup -> formatMetricGroup));
        }
    }

    public static <T> void initializeSerializationSchema(
            SerializationSchema<T> serializationSchema,
            RuntimeContext runtimeContext,
            Map<String, String> tags) throws Exception {
        FormatMetricGroup formatMetricGroup = new FormatMetricGroup(
                runtimeContext.getMetricGroup(),
                FormatMetricGroup.FORMAT_GROUP_NAME,
                false,
                tags);

        serializationSchema.open(
                RuntimeContextInitializationContextAdapters.serializationAdapter(runtimeContext,
                        metricGroup -> formatMetricGroup));
    }

    public static ObjectMapper getObjectMapper() {
        return new ObjectMapper()
                .enable(
                        DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES,
                        DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                        DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
                .disable(
                        SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }
}
