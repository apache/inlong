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
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONPath;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * JsonSetFunction   ->  JSON_SET(json_doc, path, val[, path, val, ...] )
 * description:
 * - return NULL if any argument is NULL or the 'json_doc' argument is not a valid JSON document or any path argument
 *          is not a valid path expression or contains a * or ** wildcard;
 * - return the result of inserting or updating data in 'json_doc'.
 */
@TransformFunction(type = FunctionConstant.JSON_TYPE, names = {
        "json_set"}, parameter = "(String json_doc, String path1, String val1[, String path2, String val2, ...] )", descriptions = {
                "- Return \"\" if any argument is NULL or the 'json_doc' argument is not a valid JSON document or any "
                        +
                        "path argument is not a valid path expression or contains a * or ** wildcard;",
                "- Return the result of inserting or updating data in 'json_doc'."
        }, examples = {"json_set({\\\"name\\\":\\\"Alice\\\"},\"$.name\",\"inlong\") = {\"name\":\"inlong\"}"})
public class JsonSetFunction implements ValueParser {

    private ValueParser jsonDocParser;
    private List<ValueParser> pathValuePairsParser;

    public JsonSetFunction(Function expr) {
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
        return jsonSet(jsonDocObj.toString(), pathValuePairs);
    }

    private String jsonSet(String jsonDoc, ArrayList<Object> pathValuePairs) {
        if (jsonDoc == null || pathValuePairs == null || pathValuePairs.size() % 2 != 0) {
            return null;
        }

        Object json = JSON.parse(jsonDoc);

        for (int i = 0; i < pathValuePairs.size(); i += 2) {
            String path = (String) pathValuePairs.get(i);
            Object value = pathValuePairs.get(i + 1);

            if (path == null || path.contains("*") || path.contains("**")) {
                throw new IllegalArgumentException("Invalid path expression: " + path);
            }
            if ("$".equals(path)) {
                json = JSON.parse(value.toString());
                continue;
            }
            // Insert or update data, if the path does not exist, insert new data
            try {
                JSONPath.set(json, path, JSON.parse(value.toString()));
            } catch (Exception ignored) {
                JSONPath.set(json, path, value);
            }
        }

        return json.toString();
    }
}
