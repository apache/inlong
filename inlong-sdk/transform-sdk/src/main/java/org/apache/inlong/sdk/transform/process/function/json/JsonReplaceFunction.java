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
 * JsonReplaceFunction   ->  JSON_REPLACE(json_doc, path1, val1[, path2, val2, ...] ):
 * description:
 * - Return NULL if any argument is NULL;
 * - Return the result of replacing existing values in 'json_doc'.
 * Note: An error occurs if the 'json_doc' argument is not a valid JSON document or any path argument is not a valid
 *       path expression or contains a * or ** wildcard.
 */
@TransformFunction(type = FunctionConstant.JSON_TYPE, names = {
        "json_replace"}, parameter = "(String json_doc, String path1, String val1[, String path2, String val2, ...] )", descriptions = {
                "- Return \"\" if any argument is NULL or the 'json_doc' argument is not a valid JSON document or any "
                        +
                        "path argument is not a valid path expression or contains a * or ** wildcard;",
                "- Return the result of replacing existing values in 'json_doc'."
        }, examples = {
                "json_replace(\"{ \\\"a\\\": 1, \\\"b\\\": [2, 3]}\", \"$.a\", 10, \"$.c\", \"[true, false]\") = " +
                        "{\"a\": 10, \"b\": [2, 3]}"})
public class JsonReplaceFunction implements ValueParser {

    private ValueParser jsonDocParser;
    private List<ValueParser> pathValuePairsParser;

    public JsonReplaceFunction(Function expr) {
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
        return jsonReplace(jsonDocObj.toString(), pathValuePairs);
    }

    private String jsonReplace(String jsonDoc, ArrayList<Object> pathValuePairs) {
        if (jsonDoc == null || pathValuePairs == null || pathValuePairs.size() % 2 != 0) {
            return null;
        }

        Object json = JSON.parse(jsonDoc);

        for (int i = 0; i < pathValuePairs.size(); i += 2) {
            String path = (String) pathValuePairs.get(i);
            Object value = pathValuePairs.get(i + 1);

            // If the path is' $', replace the entire JSON directly
            if (path.equals("$")) {
                try {
                    json = JSON.parse(value.toString());
                } catch (Exception ignored) {
                    json = value;
                }
            } else if (path.contains("*") || path.contains("**")) {
                throw new IllegalArgumentException("Invalid path expression: " + path);
            } else {
                // If the path exists, replace the corresponding value. If the path does not exist, ignore it
                if (JSONPath.contains(json, path)) {
                    try {
                        JSONPath.set(json, path, JSON.parse(value.toString()));
                    } catch (Exception ignored) {
                        JSONPath.set(json, path, value);
                    }
                }
            }
        }
        return json.toString();
    }

}
