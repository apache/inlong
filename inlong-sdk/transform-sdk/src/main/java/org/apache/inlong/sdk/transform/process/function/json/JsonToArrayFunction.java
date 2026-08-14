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
import org.apache.flink.table.data.binary.BinaryStringData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JsonToArrayFunction  ->  json_to_array(path)
 * description:
 * - Only works on JSON source data; returns NULL if the source is not a JsonSourceData.
 * - Returns NULL if 'path' is missing/invalid, or the path cannot be resolved to a
 *   JSON array (JsonArray).
 * - When 'path' resolves to a JSON array, returns a GenericArrayData containing the
 *   complete array. Each element is fully converted:
 *   <ul>
 *     <li>JsonObject → GenericRowData with all fields in original order</li>
 *     <li>JsonPrimitive String → {@link String}</li>
 *     <li>JsonPrimitive Boolean → {@link Boolean}</li>
 *     <li>JsonPrimitive Number → {@link Number}</li>
 *     <li>Nested JsonArray → {@link GenericArrayData} (recursively converted)</li>
 *     <li>JsonNull → {@code null}</li>
 *   </ul>
 * - No field filtering is applied; all fields from each element are included.
 */
@TransformFunction(type = FunctionConstant.JSON_TYPE, names = {
        "json_to_array"}, parameter = "(path)", descriptions = {
                "- Only works on JSON source data; returns NULL if the source is not a JsonSourceData;",
                "- Returns NULL if 'path' is missing/invalid, or the path cannot be resolved "
                        + "to a JSON array (JsonArray);",
                "- When 'path' resolves to a JSON array, returns a GenericArrayData containing "
                        + "the complete array with all elements fully converted.",
                "- JsonObject elements are converted to GenericRowData with all fields preserved.",
                "- JsonPrimitive elements are converted to their Java types (String, Boolean, Number).",
                "- Nested JsonArray elements are recursively converted to GenericArrayData.",
                "- JsonNull elements are mapped to null.",
                "- No field filtering is applied."
        }, examples = {
                "json_to_array($root.items) = <GenericArrayData of fully converted items>"
        })
public class JsonToArrayFunction implements ValueParser {

    private final ValueParser pathParser;
    private String path;

    public JsonToArrayFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        this.pathParser = OperatorTools.buildParser(expressions.get(0));
        if (pathParser instanceof ColumnParser) {
            this.path = ((ColumnParser) pathParser).getFieldName();
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (!(sourceData instanceof JsonSourceData)) {
            return null;
        }
        JsonSourceData jsonData = (JsonSourceData) sourceData;

        if (path == null) {
            return null;
        }

        // Get the field value at the specified path
        Object fieldValue = jsonData.getField(rowIndex, path);
        if (fieldValue == null) {
            return null;
        }

        // Must be a JsonArray, otherwise return null
        if (!(fieldValue instanceof JsonArray)) {
            return null;
        }

        return buildArray((JsonArray) fieldValue);
    }

    /**
     * Build a GenericArrayData from a JsonArray.
     * Each element is fully converted to its corresponding Java type or Flink data structure.
     */
    private GenericArrayData buildArray(JsonArray jsonArray) {
        List<Object> valueResult = new ArrayList<>(jsonArray.size());
        for (int i = 0; i < jsonArray.size(); i++) {
            JsonElement element = jsonArray.get(i);
            valueResult.add(convertJsonElement(element));
        }
        return new GenericArrayData(valueResult.toArray());
    }

    /**
     * Convert a JsonElement to its corresponding Java type or Flink data structure.
     * <ul>
     *   <li>JsonPrimitive String → {@link String}</li>
     *   <li>JsonPrimitive Boolean → {@link Boolean}</li>
     *   <li>JsonPrimitive Number → {@link Number}</li>
     *   <li>JsonObject → {@link GenericRowData} with all fields preserved</li>
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
                return BinaryStringData.fromString(jsonPrim.getAsString());
            }
            if (jsonPrim.isBoolean()) {
                return jsonPrim.getAsBoolean();
            }
            if (jsonPrim.isNumber()) {
                return jsonPrim.getAsNumber();
            }
            return BinaryStringData.fromString(jsonPrim.getAsString());
        }
        if (element.isJsonObject()) {
            return buildRow(element.getAsJsonObject());
        }
        if (element.isJsonArray()) {
            return buildArray(element.getAsJsonArray());
        }
        return element.toString();
    }

    /**
     * Build a GenericRowData from a JsonObject with all fields preserved,
     * in the original insertion order within the JsonObject.
     */
    private GenericRowData buildRow(JsonObject jsonObject) {
        int fieldCount = jsonObject.size();
        GenericRowData result = new GenericRowData(fieldCount);
        int index = 0;
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            Object value = convertJsonElement(entry.getValue());
            result.setField(index++, value);
        }
        return result;
    }
}
