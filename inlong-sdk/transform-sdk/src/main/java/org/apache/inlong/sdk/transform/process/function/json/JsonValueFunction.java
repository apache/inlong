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

import com.alibaba.fastjson.JSONPath;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

/**
 * JsonValueFunction  ->  JSON_VALUE(json_doc, path)
 * description:
 * - Return NULL if any parameter is NULL
 * - Return the scalar extracted from JSON string (json_doc) based on path
 */
@TransformFunction(names = {"json_value"}, parameter = "(String json_doc,String path)", descriptions = {
        "- Return \"\" if any parameter is NULL;",
        "- Return the scalar extracted from JSON string ('json_doc') based on 'path'."
}, examples = {
        "json_value({\"a\": 1}, $.a) = 1",
        "json_value({\\\"person\\\": {\\\"name\\\": \\\"Alice\\\" ,\\\"age\\\": 30}}, $.person.name) = Alice"
})
public class JsonValueFunction implements ValueParser {

    private final ValueParser jsonParser;

    private final ValueParser pathParser;

    public JsonValueFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        this.jsonParser = OperatorTools.buildParser(expressions.get(0));
        this.pathParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object jsonObj = jsonParser.parse(sourceData, rowIndex, context);
        Object pathObj = pathParser.parse(sourceData, rowIndex, context);
        if (jsonObj == null || pathObj == null) {
            return null;
        }
        String path = OperatorTools.parseString(pathObj);
        String json = OperatorTools.parseString(jsonObj);

        if (path.isEmpty()) {
            return null;
        }
        Object res = JSONPath.read(json, path);
        // check if it is a scalar
        if (res instanceof String || res instanceof Number || res instanceof Boolean) {
            return res.toString();
        }
        return null;
    }
}
