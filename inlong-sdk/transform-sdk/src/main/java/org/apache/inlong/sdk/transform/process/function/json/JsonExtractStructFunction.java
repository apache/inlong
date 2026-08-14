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
import java.util.List;

/**
 * JsonExtractStructFunction  ->  json_extract_struct(path, field1, field2, ...)
 * description:
 * - Only works on JSON source data; returns NULL if the source is not a JsonSourceData.
 * - Returns NULL if 'path' is missing/invalid, or the path cannot be resolved to a
 *   JSON object or JSON array of objects.
 * - When 'path' resolves to a JSON object, returns a GenericRowData containing the
 *   specified fields in order. Each field is extracted from the JSON object by name;
 *   fields that cannot be resolved are set to NULL.
 * - When 'path' resolves to a JSON array of objects, returns a GenericArrayData whose
 *   elements are GenericRowData for each array element.
 * - Nested json_extract_struct is supported: e.g.
 *   json_extract_struct(json_extract_struct($root.person, name, age), name)
 */
@TransformFunction(type = FunctionConstant.JSON_TYPE, names = {
        "json_extract_struct"}, parameter = "(path, field1, field2, ...)", descriptions = {
                "- Only works on JSON source data; returns NULL if the source is not a JsonSourceData;",
                "- Returns NULL if 'path' is missing/invalid, or the path cannot be resolved "
                        + "to a JSON object or array;",
                "- When 'path' resolves to a JSON object, returns a GenericRowData containing "
                        + "the specified fields in order;",
                "- When 'path' resolves to a JSON array of objects, returns a GenericArrayData "
                        + "whose elements are GenericRowData for each array element.",
                "- Nested json_extract_struct is supported."
        }, examples = {
                "json_extract_struct($root.person,name,age) = <GenericRowData with fields [name, age]>"
        })
public class JsonExtractStructFunction implements ValueParser {

    private final ValueParser pathParser;
    private final List<ValueParser> fieldParsers;
    private String path;
    private boolean isNestedStruct = false;
    private boolean isKeepMessage = false;

    public JsonExtractStructFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        this.pathParser = OperatorTools.buildParser(expressions.get(0));
        if (pathParser instanceof ColumnParser) {
            this.path = ((ColumnParser) pathParser).getFieldName();
        } else if (pathParser instanceof JsonExtractStructFunction) {
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

        // Handle nested json_extract_struct as path
        if (isNestedStruct) {
            JsonExtractStructFunction child = (JsonExtractStructFunction) pathParser;
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

        // Handle JsonObject: build a single GenericRowData
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

        // Handle GenericRowData (from nested json_extract_struct)
        if (fieldValue instanceof GenericRowData) {
            return fieldValue;
        }

        // Handle GenericArrayData (from nested json_extract_struct)
        if (fieldValue instanceof GenericArrayData) {
            return fieldValue;
        }

        return null;
    }

    /**
     * Build a GenericRowData from a JsonObject using the declared field parsers.
     * Each field parser extracts a value from the JsonObject by its field name.
     * Supports nested paths (e.g., "address.street") by traversing the JSON tree.
     */
    private GenericRowData buildStruct(JsonObject jsonObject, int rowIndex, Context context) {
        GenericRowData result = new GenericRowData(fieldParsers.size());
        int index = 0;
        for (ValueParser parser : fieldParsers) {
            if (parser instanceof ColumnParser) {
                ColumnParser columnParser = (ColumnParser) parser;
                String fieldName = columnParser.getFieldName();
                Object value = getJsonFieldValue(jsonObject, fieldName);
                result.setField(index++, value);
            } else if (parser instanceof JsonExtractStructFunction) {
                // Nested json_extract_struct as a field value
                Object value = ((JsonExtractStructFunction) parser).buildFromJsonObject(
                        jsonObject, rowIndex, context);
                result.setField(index++, value);
            } else {
                result.setField(index++, null);
            }
        }
        return result;
    }

    /**
     * Build a GenericArrayData from a JsonArray where each element is expected
     * to be a JsonObject. Each JsonObject is converted to GenericRowData via buildStruct.
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
            // Return the raw JsonObject; caller can extract sub-fields via nested json_extract_struct
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
     */
    public boolean isKeepMessage() {
        return isKeepMessage;
    }

    /**
     * Set the keep-message flag.
     * When true, parse() returns the raw JsonObject/JsonArray instead of GenericRowData.
     */
    public void setKeepMessage(boolean isKeepMessage) {
        this.isKeepMessage = isKeepMessage;
    }
}
