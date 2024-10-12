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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * JsonInsertFunction   ->  JSON_INSERT(json_doc, path, val[, path, val, ...])
 * description:
 * - return NULL if any argument is NULL or the 'json_doc' argument is not a valid JSON document
 *          or any path argument is not a valid path expression or contains a * or ** wildcard;
 * - return the result of inserting data into 'json_doc'.
 */
@TransformFunction(names = {
        "json_insert"}, parameter = "(String json_doc, String path1, String val1[, String path2, String val2, ...] )", descriptions = {
                "- Return \"\" if any argument is NULL or the 'json_doc' argument is not a valid JSON document " +
                        "or any path argument is not a valid path expression or contains a * or ** wildcard.;",
                "- Return the result of inserting data into 'json_doc'."
        }, examples = {
                "json_insert({\"a\": {\"b\": [1, 2]}, \"c\": [3, 4]}, $.c[1][1], \"2\", \"$.c[1][1][5]\", \"1\") = " +
                        "{\"a\":{\"b\":[1,2]},\"c\":[3,[4,[\"2\",\"1\"]]]}"
        })
public class JsonInsertFunction implements ValueParser {

    private final ValueParser jsonDocParser;
    private final List<ValueParser> pathValuePairsParser;

    public JsonInsertFunction(Function expr) {
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
        return jsonInsert(jsonDocObj.toString(), pathValuePairs);
    }

    private static String jsonInsert(String json, ArrayList<Object> pathValuePairs) {
        if (json == null || pathValuePairs == null || pathValuePairs.size() % 2 != 0) {
            return null;
        }

        JSONObject jsonObject;
        try {
            jsonObject = JSON.parseObject(json);
        } catch (Exception e) {
            // Invalid JSON document
            return null;
        }

        for (int i = 0; i < pathValuePairs.size(); i += 2) {
            String path = (String) pathValuePairs.get(i);
            Object value = pathValuePairs.get(i + 1);

            if (path == null || value == null) {
                return null;
            }

            if (path.contains("*") || path.contains("**")) {
                throw new IllegalArgumentException("Invalid path expression: " + path);
            }

            List<Object> tokens = parsePath(path);
            if (tokens.isEmpty()) {
                continue;
            }

            Object current = jsonObject;
            Object parent = null, currentToken = null;

            for (int j = 0; j < tokens.size() - 1; j++) {
                Object token = tokens.get(j);
                parent = current;
                currentToken = token;

                if (token instanceof String) {
                    String key = (String) token;
                    if (!(current instanceof JSONObject)) {
                        JSONObject newObj = new JSONObject();
                        if (parent instanceof JSONObject) {
                            ((JSONObject) parent).put((String) currentToken, newObj);
                        } else if (parent instanceof JSONArray) {
                            ((JSONArray) parent).set((Integer) currentToken, newObj);
                        }
                        current = newObj;
                    }
                    JSONObject jsonObj = (JSONObject) current;

                    Object next = jsonObj.get(key);
                    if (next == null) {
                        Object nextToken = tokens.get(j + 1);
                        next = nextToken instanceof Integer ? new JSONArray() : new JSONObject();
                        jsonObj.put(key, next);
                    }
                    current = next;
                } else if (token instanceof Integer) {
                    int index = (Integer) token;
                    if (!(current instanceof JSONArray)) {
                        JSONArray newArr = new JSONArray();
                        newArr.add(current);
                        if (parent instanceof JSONObject) {
                            ((JSONObject) parent).put((String) currentToken, newArr);
                        } else if (parent instanceof JSONArray) {
                            ((JSONArray) parent).set((Integer) currentToken, newArr);
                        }
                        current = newArr;
                    }
                    JSONArray jsonArr = (JSONArray) current;
                    if (jsonArr.size() <= index) {
                        index = jsonArr.size();
                    }
                    Object next = jsonArr.get(index);
                    if (next == null) {
                        Object nextToken = tokens.get(j + 1);
                        next = nextToken instanceof Integer ? new JSONArray() : new JSONObject();
                        jsonArr.set(index, next);
                    }
                    current = next;
                }
            }

            Object lastToken = tokens.get(tokens.size() - 1);
            if (lastToken instanceof String) {
                String key = (String) lastToken;
                if (!(current instanceof JSONObject)) {
                    JSONObject newObj = new JSONObject();
                    if (parent instanceof JSONObject) {
                        ((JSONObject) parent).put((String) currentToken, newObj);
                    } else if (parent instanceof JSONArray) {
                        ((JSONArray) parent).set((Integer) currentToken, newObj);
                    }
                    current = newObj;
                }
                JSONObject jsonObj = (JSONObject) current;
                if (!jsonObj.containsKey(key)) {
                    jsonObj.put(key, parseValue(value));
                }
            } else if (lastToken instanceof Integer) {
                int index = (Integer) lastToken;
                if (!(current instanceof JSONArray)) {
                    JSONArray newArr = new JSONArray();
                    newArr.add(current);
                    if (parent instanceof JSONObject) {
                        ((JSONObject) parent).put((String) currentToken, newArr);
                    } else if (parent instanceof JSONArray) {
                        ((JSONArray) parent).set((Integer) currentToken, newArr);
                    }
                    current = newArr;
                }
                JSONArray jsonArr = (JSONArray) current;
                if (index >= jsonArr.size() || jsonArr.get(index) == null) {
                    if (jsonArr.size() <= index) {
                        index = jsonArr.size();
                    }
                    jsonArr.set(index, parseValue(value));
                }
            }
        }

        return jsonObject.toJSONString();
    }

    private static Object parseValue(Object value) {
        if (value instanceof String) {
            String str = (String) value;
            try {
                // Try to parse as JSON
                Object json = JSON.parse(str);
                if (json instanceof JSONObject || json instanceof JSONArray) {
                    return json;
                }
            } catch (Exception ignored) {
                // Not a JSON string
            }
        }
        return value;
    }

    private static List<Object> parsePath(String path) {
        if (path == null || !path.startsWith("$")) {
            throw new IllegalArgumentException("Invalid path expression: " + path);
        }

        List<Object> tokens = new ArrayList<>();
        int i = 1; // Skip the '$'
        int len = path.length();

        while (i < len) {
            char c = path.charAt(i);
            if (c == '.') {
                i++;
                int start = i;
                while (i < len && path.charAt(i) != '.' && path.charAt(i) != '[') {
                    i++;
                }
                if (start == i) {
                    throw new IllegalArgumentException("Invalid path expression: " + path);
                }
                String key = path.substring(start, i);
                tokens.add(key);
            } else if (c == '[') {
                i++;
                int start = i;
                while (i < len && path.charAt(i) != ']') {
                    i++;
                }
                if (i == len || path.charAt(i) != ']') {
                    throw new IllegalArgumentException("Invalid path expression: " + path);
                }
                String indexStr = path.substring(start, i);
                i++; // Skip ']'
                try {
                    int index = Integer.parseInt(indexStr);
                    tokens.add(index);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid array index in path: " + path);
                }
            } else {
                throw new IllegalArgumentException("Invalid path expression: " + path);
            }
        }
        return tokens;
    }
}
