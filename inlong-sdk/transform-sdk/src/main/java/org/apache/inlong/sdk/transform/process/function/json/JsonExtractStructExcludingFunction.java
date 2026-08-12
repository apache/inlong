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

package org.apache.inlong.sdk.transform.process.function.json;

import org.apache.inlong.sdk.transform.decode.JsonSourceData;
import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ColumnParser;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * JsonExtractStructExcludingFunction  ->  json_extract_struct_excluding(path, excludeField1, excludeField2, ...)
 * description:
 * - Only works on JSON source data; returns NULL if the source is not a JsonSourceData.
 * - Returns NULL if 'path' is missing/invalid, or the path cannot be resolved to a
 *   JSON object or JSON array of objects.
 * - When 'path' resolves to a JSON object, returns a GenericRowData containing all
 *   fields except the specified excluded fields, in the original order.
 * - When 'path' resolves to a JSON array of objects, returns a GenericArrayData whose
 *   elements are GenericRowData for each array element (all fields except the excluded ones).
 * - Nested json_extract_struct_excluding is supported: e.g.
 *   json_extract_struct_excluding(json_extract_struct_excluding($root.person, address), phone)
 */
@TransformFunction(type = FunctionConstant.JSON_TYPE, names = {
        "json_extract_struct_excluding"}, parameter = "(path, excludeField1, excludeField2, ...)", descriptions = {
                "- Only works on JSON source data; returns NULL if the source is not a JsonSourceData;",
                "- Returns NULL if 'path' is missing/invalid, or the path cannot be resolved "
                        + "to a JSON object or array;",
                "- When 'path' resolves to a JSON object, returns a GenericRowData containing "
                        + "all fields except the specified excluded fields;",
                "- When 'path' resolves to a JSON array of objects, returns a GenericArrayData "
                        + "whose elements are GenericRowData for each array element (all fields except excluded).",
                "- Nested json_extract_struct_excluding is supported."
        }, examples = {
                "json_extract_struct_excluding($root.person,address,phone) "
                        + "= <GenericRowData of person without address and phone>"
        })
public class JsonExtractStructExcludingFunction implements ValueParser {

    private final ValueParser pathParser;
    private final List<ValueParser> fieldParsers;
    private String path;
    private boolean isNestedStruct = false;
    private boolean isKeepMessage = false;

    public JsonExtractStructExcludingFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        this.pathParser = OperatorTools.buildParser(expressions.get(0));
        if (pathParser instanceof ColumnParser) {
            this.path = ((ColumnParser) pathParser).getFieldName();
        } else if (pathParser instanceof JsonExtractStructExcludingFunction) {
            this.isNestedStruct = true;
        }
        this.fieldParsers = new ArrayList<>();
        for (int i = 1; i < expressions.size(); i++) {
            this.fieldParsers.add(OperatorTools.buildParser(expressions.get(i)));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (!(sourceData instanceof JsonSourceData)) {
            return null;
        }
        JsonSourceData jsonData = (JsonSourceData) sourceData;

        // Handle nested json_extract_struct_excluding as path
        if (isNestedStruct) {
            JsonExtractStructExcludingFunction child = (JsonExtractStructExcludingFunction) pathParser;
            child.setKeepMessage(true);
            Object nestedResult = child.parse(sourceData, rowIndex, context);
            child.setKeepMessage(false);
            if (nestedResult instanceof JsonObject) {
                return buildStruct((JsonObject) nestedResult, rowIndex, context);
            }
            if (nestedResult instanceof JsonArray) {
                return buildArrayStruct((JsonArray) nestedResult, rowIndex, context);
            }
            if (nestedResult instanceof GenericArrayData) {
                return nestedResult;
            }
            return null;
        }

        if (path == null) {
            return null;
        }

        // Get the field value at the specified path
        Object fieldValue = jsonData.getField(rowIndex, path);
        if (fieldValue == null) {
            return null;
        }

        // Handle JsonObject: build a single GenericRowData (all fields except excluded)
        if (fieldValue instanceof JsonObject) {
            if (isKeepMessage()) {
                return fieldValue;
            }
            return buildStruct((JsonObject) fieldValue, rowIndex, context);
        }

        // Handle JsonArray: build GenericArrayData of GenericRowData
        if (fieldValue instanceof JsonArray) {
            if (isKeepMessage()) {
                return fieldValue;
            }
            return buildArrayStruct((JsonArray) fieldValue, rowIndex, context);
        }

        // Primitive or null JsonElement: not a struct
        if (fieldValue instanceof JsonElement) {
            return null;
        }

        // Handle GenericRowData (from nested json_extract_struct_excluding)
        if (fieldValue instanceof GenericRowData) {
            return fieldValue;
        }

        // Handle GenericArrayData (from nested json_extract_struct_excluding)
        if (fieldValue instanceof GenericArrayData) {
            return fieldValue;
        }

        return null;
    }

    /**
     * Build a GenericRowData from a JsonObject, excluding the specified fields.
     * All fields except the excluded ones are included in the result, in their
     * original insertion order within the JsonObject.
     */
    private GenericRowData buildStruct(JsonObject jsonObject, int rowIndex, Context context) {
        // Collect excluded field names from the column parsers
        Set<String> excludedFields = new HashSet<>();
        for (ValueParser parser : fieldParsers) {
            if (parser instanceof ColumnParser) {
                excludedFields.add(((ColumnParser) parser).getFieldName());
            }
        }

        // Collect non-excluded entries in order
        List<Map.Entry<String, JsonElement>> includedEntries = new ArrayList<>();
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            if (!excludedFields.contains(entry.getKey())) {
                includedEntries.add(entry);
            }
        }

        GenericRowData result = new GenericRowData(includedEntries.size());
        int index = 0;
        for (Map.Entry<String, JsonElement> entry : includedEntries) {
            Object value = convertJsonElement(entry.getValue());
            result.setField(index++, value);
        }
        return result;
    }

    /**
     * Build a GenericArrayData from a JsonArray where each element is expected
     * to be a JsonObject. Each JsonObject is converted to GenericRowData via buildStruct,
     * with the excluded fields removed.
     */
    private GenericArrayData buildArrayStruct(JsonArray jsonArray, int rowIndex, Context context) {
        List<Object> valueResult = new ArrayList<>(jsonArray.size());
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonElement element = jsonArray.get(i);
            if (element.isJsonObject()) {
                valueResult.add(buildStruct(element.getAsJsonObject(), rowIndex, context));
            } else if (element.isJsonNull()) {
                valueResult.add(null);
            } else {
                // Primitive element in array: convert to its Java type
                valueResult.add(convertJsonElement(element));
            }
        }
        return new GenericArrayData(valueResult.toArray());
    }

    /**
     * Convert a JsonElement to its corresponding Java type or Flink data structure.
     * <ul>
     *   <li>JsonPrimitive String → {@link String}</li>
     *   <li>JsonPrimitive Boolean → {@link Boolean}</li>
     *   <li>JsonPrimitive Number → {@link Number}</li>
     *   <li>JsonObject → the original {@link JsonObject} (for further processing)</li>
     *   <li>JsonArray → {@link GenericArrayData} with each element converted recursively</li>
     *   <li>JsonNull → {@code null}</li>
     * </ul>
     */
    private Object convertJsonElement(JsonElement element) {
        if (element == null || element.isJsonNull()) {
            return null;
        }
        if (element.isJsonPrimitive()) {
            JsonPrimitive jsonPrim = element.getAsJsonPrimitive();
            if (jsonPrim.isString()) {
                return jsonPrim.getAsString();
            }
            if (jsonPrim.isBoolean()) {
                return jsonPrim.getAsBoolean();
            }
            if (jsonPrim.isNumber()) {
                return jsonPrim.getAsNumber();
            }
            return jsonPrim.getAsString();
        }
        if (element.isJsonObject()) {
            // Return the raw JsonObject; caller can extract sub-fields via nested json_extract_struct_excluding
            return element.getAsJsonObject();
        }
        if (element.isJsonArray()) {
            JsonArray jsonArray = element.getAsJsonArray();
            List<Object> list = new ArrayList<>(jsonArray.size());
            for (int i = 0; i < jsonArray.size(); i++) {
                list.add(convertJsonElement(jsonArray.get(i)));
            }
            return new GenericArrayData(list.toArray());
        }
        return element.toString();
    }

    /**
     * Extract a field value from a JsonObject, supporting nested paths
     * separated by dots (e.g., "address.street").
     */
    private Object getJsonFieldValue(JsonObject jsonObject, String fieldName) {
        String[] parts = fieldName.split("\\.");
        JsonElement current = jsonObject;
        for (int i = 0; i < parts.length; i++) {
            if (current == null || !current.isJsonObject()) {
                return null;
            }
            current = current.getAsJsonObject().get(parts[i]);
            if (current == null || current.isJsonNull()) {
                return null;
            }
        }
        return convertJsonElement(current);
    }

    /**
     * Build struct data from a raw JsonObject (used internally for nested struct processing).
     */
    Object buildFromJsonObject(JsonObject jsonObject, int rowIndex, Context context) {
        if (this.path != null) {
            // If this function has its own path, resolve it relative to the given JsonObject
            Object value = getJsonFieldValue(jsonObject, this.path);
            if (value instanceof JsonObject) {
                return buildStruct((JsonObject) value, rowIndex, context);
            }
            if (value instanceof JsonArray) {
                return buildArrayStruct((JsonArray) value, rowIndex, context);
            }
            return null;
        }
        return buildStruct(jsonObject, rowIndex, context);
    }

    /**
     * Check whether the keep-message flag is set.
     * When true, parse() returns the raw JsonObject/JsonArray instead of GenericRowData.
     *
     * @return the isKeepMessage
     */
    public boolean isKeepMessage() {
        return isKeepMessage;
    }

    /**
     * Set the keep-message flag.
     * When true, parse() returns the raw JsonObject/JsonArray instead of GenericRowData.
     *
     * @param isKeepMessage the isKeepMessage to set
     */
    public void setKeepMessage(boolean isKeepMessage) {
        this.isKeepMessage = isKeepMessage;
    }
}
