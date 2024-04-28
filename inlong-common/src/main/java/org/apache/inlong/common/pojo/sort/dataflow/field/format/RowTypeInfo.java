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

package org.apache.inlong.common.pojo.sort.dataflow.field.format;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * The type information for rows.
 */
public class RowTypeInfo implements TypeInfo {

    private static final long serialVersionUID = 1L;

    private static final String FIELD_FIELD_NAMES = "fieldNames";
    private static final String FIELD_FIELD_TYPES = "fieldTypes";
    private static final String FIELD_FIELD_DESCRIPTIONS = "fieldDescriptions";

    public static final RowTypeInfo EMPTY = new RowTypeInfo(
            new String[0],
            new TypeInfo[0],
            new String[0]);

    @JsonProperty(FIELD_FIELD_NAMES)
    @Nonnull
    private final String[] fieldNames;

    @JsonProperty(FIELD_FIELD_TYPES)
    @Nonnull
    private final TypeInfo[] fieldTypeInfos;

    @JsonProperty(FIELD_FIELD_DESCRIPTIONS)
    @Nonnull
    private final String[] fieldDescriptions;

    @JsonCreator
    public RowTypeInfo(
            @JsonProperty(FIELD_FIELD_NAMES) @Nonnull String[] fieldNames,
            @JsonProperty(FIELD_FIELD_TYPES) @Nonnull TypeInfo[] fieldTypeInfos,
            @JsonProperty(FIELD_FIELD_DESCRIPTIONS) @Nonnull String[] fieldDescriptions) {
        checkArity(fieldNames, fieldTypeInfos, fieldDescriptions);
        checkDuplicates(fieldNames);

        this.fieldNames = fieldNames;
        this.fieldTypeInfos = fieldTypeInfos;
        this.fieldDescriptions = fieldDescriptions;
    }

    private static void checkArity(
            String[] fieldNames,
            TypeInfo[] fieldTypeInfos,
            String[] fieldDescriptions) {
        if (fieldNames.length != fieldTypeInfos.length) {
            throw new IllegalArgumentException(String.format("The number of names and " +
                    "types is not equal. FieldNames: %s, filedTypes: %s",
                    Arrays.toString(fieldNames), Arrays.toString(fieldTypeInfos)));
        }
        if (fieldDescriptions.length != fieldTypeInfos.length) {
            throw new IllegalArgumentException(String.format("The number of descriptions and " +
                    "types is not equal. FieldDescriptions: %s, filedTypes: %s",
                    Arrays.toString(fieldDescriptions), Arrays.toString(fieldTypeInfos)));
        }
        if (fieldDescriptions.length != fieldNames.length) {
            throw new IllegalArgumentException(String.format("The number of descriptions and " +
                    "names is not equal. FieldDescriptions: %s, filedNames: %s",
                    Arrays.toString(fieldDescriptions), Arrays.toString(fieldNames)));
        }
    }

    @Nonnull
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Nonnull
    public TypeInfo[] getFieldTypeInfos() {
        return fieldTypeInfos;
    }

    @Nonnull
    public String[] getFieldDescriptions() {
        return fieldDescriptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RowTypeInfo that = (RowTypeInfo) o;
        return Arrays.equals(fieldNames, that.fieldNames) &&
                Arrays.equals(fieldTypeInfos, that.fieldTypeInfos) &&
                Arrays.equals(fieldDescriptions, that.fieldDescriptions);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(fieldTypeInfos);
    }

    @Override
    public String toString() {
        return "RowTypeInfo{" +
                "fieldNames=" + Arrays.toString(fieldNames) +
                "fieldTypeInfos=" + Arrays.toString(fieldTypeInfos) +
                "fieldDescriptions=" + Arrays.toString(fieldDescriptions) +
                '}';
    }

    private static void checkDuplicates(String[] fieldNames) {
        long numFieldNames = fieldNames.length;
        long numDistinctFieldNames =
                Arrays.stream(fieldNames)
                        .collect(Collectors.toSet())
                        .size();

        if (numDistinctFieldNames != numFieldNames) {
            throw new IllegalArgumentException("There exist duplicated " + "field names.");
        }
    }
}
