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
import com.alibaba.fastjson.JSONPath;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * JsonRemoveFunction   ->  JSON_REMOVE(json_doc, path[, path] ...)
 * description:
 * - return NULL if any argument is NULL or the 'json_doc' argument is not a valid JSON document or any path argument
 *          is not a valid path expression or is $ or contains a * or ** wildcard;
 * - return the result of removing data from 'json_doc'.
 */
@TransformFunction(names = {
        "json_remove"}, parameter = "(String json_doc, String path1[, String path2, ...])", descriptions = {
                "- Return \"\" if any argument is NULL or the 'json_doc' argument is not a valid JSON document or any "
                        +
                        "path argument is not a valid path expression or is $ or contains a * or ** wildcard;",
                "- Return the result of removing data from 'json_doc'."
        }, examples = {
                "json_remove(\"{\\\"name\\\":\\\"Charlie\\\",\\\"hobbies\\\":[[\\\"swimming1\\\",\\\"swimming2\\\"]," +
                        "\\\"reading\\\",\\\"coding\\\"]}\",\"$.age\") = {\"hobbies\":[[\"swimming2\"],\"coding\"]," +
                        "\"name\":\"Charlie\"}"
        })
public class JsonRemoveFunction implements ValueParser {

    private ValueParser jsonDocParser;
    private List<ValueParser> pathsParser;

    public JsonRemoveFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        jsonDocParser = OperatorTools.buildParser(expressions.get(0));
        pathsParser = new ArrayList<>();
        for (int i = 1; i < expressions.size(); i++) {
            pathsParser.add(OperatorTools.buildParser(expressions.get(i)));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object jsonDocObj = jsonDocParser.parse(sourceData, rowIndex, context);
        if (jsonDocObj == null) {
            return null;
        }
        ArrayList<String> pathValuePairs = new ArrayList<>();
        for (ValueParser valueParser : pathsParser) {
            pathValuePairs.add(valueParser.parse(sourceData, rowIndex, context).toString());
        }
        return jsonRemove(jsonDocObj.toString(), pathValuePairs);
    }

    private String jsonRemove(String jsonDoc, ArrayList<String> paths) {
        if (jsonDoc == null || paths == null) {
            return null;
        }

        Object json = JSON.parse(jsonDoc);

        for (String path : paths) {
            if (path.equals("$") || path.contains("*") || path.contains("**")) {
                throw new IllegalArgumentException("Invalid path expression: " + path);
            }

            JSONPath.remove(json, path);
        }

        return json.toString();
    }
}
