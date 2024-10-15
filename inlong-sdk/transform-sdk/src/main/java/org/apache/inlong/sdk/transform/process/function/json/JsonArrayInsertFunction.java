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

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * JsonArrayInsertFunction   ->  JSON_ARRAY_INSERT(json_doc, path, val[, path, val] ...)
 * description:
 * - return NULL if any argument is NULL;
 * - return the document inserted into the array.
 * Note: If multiple groups of parameters are passed in, the parameter subscripts of the latter groups
 * need to be based on the document subscripts after the previous group of parameters are updated.
 */
@TransformFunction(names = {
        "json_array_insert"}, parameter = "(String json_doc, String path1, String val1[, String path2, String val2, ...] )", descriptions = {
                "- Return \"\" if any argument is NULL;",
                "- Return the 'json_doc' inserted into the array.",
                "Note: If multiple groups of parameters are passed in, the parameter subscripts of the latter groups " +
                        "need to be based on the document subscripts after the previous group of parameters are updated."
        }, examples = {
                "json_array_append([\"a\", {\"b\": [1, 2]}, [3, 4]], $[1], x) = [\"a\",\"x\",{\"b\":[1,2]},[3,4]]"
        })
public class JsonArrayInsertFunction implements ValueParser {

    private ValueParser jsonDocParser;
    private List<ValueParser> pathValuePairsParser;

    public JsonArrayInsertFunction(Function expr) {
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
        return jsonArrayInsert(jsonDocObj.toString(), pathValuePairs);
    }

    private String jsonArrayInsert(String jsonDoc, ArrayList<Object> pathValuePairs) {
        if (jsonDoc == null || pathValuePairs == null || pathValuePairs.size() % 2 != 0) {
            return null;
        }

        Object jsonObject = JSON.parse(jsonDoc);

        for (int i = 0; i < pathValuePairs.size(); i += 2) {
            String path = (String) pathValuePairs.get(i);
            Object value = pathValuePairs.get(i + 1);

            if (!path.endsWith("]")) {
                throw new IllegalArgumentException("Path must end with an array index: " + path);
            }

            // Find the parent path
            String parentPath = path.substring(0, path.lastIndexOf('['));
            Object parentNode = JSONPath.eval(jsonObject, parentPath);

            if (parentNode instanceof JSONArray) {
                // If the parent path is an array, perform the insert operation
                insertIntoArray((JSONArray) parentNode, path, value);
            } else if (parentNode instanceof JSONObject) {
                // If the parent path is an object, try inserting it into the array inside the object
                String arrayIndexPart = path.substring(path.lastIndexOf('['), path.lastIndexOf(']') + 1);
                handleArrayInsertionInObject((JSONObject) parentNode, arrayIndexPart, value);
            } else {
                throw new IllegalArgumentException("Invalid path or target node is not an array or object: " + path);
            }
        }

        return JSON.toJSONString(jsonObject);
    }

    private void insertIntoArray(JSONArray array, String path, Object value) {
        String indexPart = path.substring(path.lastIndexOf('[') + 1, path.lastIndexOf(']'));
        int index = Integer.parseInt(indexPart);

        // If the index exceeds the length of the array, insert at the end
        if (index >= array.size()) {
            index = array.size();
        }
        try {
            array.add(index, JSON.parse(value.toString()));
        } catch (Exception ignored) {
            array.add(index, value);
        }
    }

    private void handleArrayInsertionInObject(JSONObject jsonObject, String arrayPart, Object value) {
        String arrayField = arrayPart.substring(1, arrayPart.length() - 1);
        JSONArray array = jsonObject.getJSONArray(arrayField);
        if (array != null) {
            insertIntoArray(array, arrayPart, value);
        }
    }
}
