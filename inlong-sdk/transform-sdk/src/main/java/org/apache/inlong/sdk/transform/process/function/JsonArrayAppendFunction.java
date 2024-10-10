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

package org.apache.inlong.sdk.transform.process.function;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONPath;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * JsonArrayAppendFunction   ->   JSON_ARRAY_APPEND(json_doc, path, val[, path, val] ...)
 * description:
 * - return NULL if any argument is NULL.
 * - return the result of appends values to the end of the indicated arrays within a JSON document.
 */
@TransformFunction(names = {"json_array_append"})
public class JsonArrayAppendFunction implements ValueParser {

    private ValueParser jsonDocParser;
    private List<ValueParser> pathValuePairsParser;

    public JsonArrayAppendFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        jsonDocParser = OperatorTools.buildParser(expressions.get(0));
        pathValuePairsParser = new ArrayList<>();
        for (int i = 1; i < expressions.size(); i++) {
            pathValuePairsParser.add(OperatorTools.buildParser(expressions.get(i)));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object jsonDocObj = jsonDocParser.parse(sourceData, rowIndex, context);
        if (jsonDocObj == null) {
            return null;
        }
        ArrayList<Object> pathValuePairs = new ArrayList<>();
        for (ValueParser valueParser : pathValuePairsParser) {
            pathValuePairs.add(valueParser.parse(sourceData, rowIndex, context));
        }
        return jsonArrayAppend(jsonDocObj.toString(), pathValuePairs);
    }

    public static String jsonArrayAppend(String jsonDoc, ArrayList<Object> pathValuePairs) {
        if (jsonDoc == null || pathValuePairs == null || pathValuePairs.size() % 2 != 0) {
            return null;
        }

        Object jsonObject;
        try {
            jsonObject = JSON.parse(jsonDoc);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid JSON document", e);
        }

        // Process each pair of path and val
        for (int i = 0; i < pathValuePairs.size(); i += 2) {
            String path = (String) pathValuePairs.get(i);
            Object value = pathValuePairs.get(i + 1);

            // Attempt to append a value to the array pointed to by the specified path
            try {
                jsonObject = appendValueToArray(jsonObject, path, value);
            } catch (Exception e) {
                throw new IllegalArgumentException("Error processing path: " + path, e);
            }
        }

        return JSON.toJSONString(jsonObject);
    }

    /**
     * Append values to an array at a specified path
     *
     * @param jsonObject The object parsed by jsonDoc
     * @param path       path
     * @param value      value
     */
    private static Object appendValueToArray(Object jsonObject, String path, Object value) {
        Object targetNode = JSONPath.eval(jsonObject, path);

        if (targetNode == null) {
            throw new IllegalArgumentException("Target path does not exist.");
        }

        // If it is already an array type, simply append it
        if (targetNode instanceof JSONArray) {
            JSONArray array = (JSONArray) targetNode;
            array.add(value);
        }
        // If it is a non array type, convert it to an array and append it
        else {
            JSONArray newArray = new JSONArray();
            newArray.add(targetNode);
            newArray.add(value);
            if ("$".equals(path)) {
                return newArray;
            }
            JSONPath.set(jsonObject, path, newArray);
        }
        return jsonObject;
    }
}
