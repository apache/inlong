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
import net.sf.jsqlparser.expression.Function;

/**
 * JsonQuoteFunction  ->  JSON_QUOTE(str) or JSON_STRING(str)
 * description:
 * - Return NULL if str is NULL
 * - Return a valid JSON string converted from a string (JSON_QUOTE) or any type of data (JSON_STRING)
 * Note: JSON_QUOTE will escape interior quote and special characters (’"’, ‘', ‘/’, ‘b’, ‘f’, ’n’, ‘r’, ’t’)
 */
@TransformFunction(names = {"json_quote", "json_string"}, parameter = "(String data)", descriptions = {
        "- Return \"\" if data is NULL;",
        "- Return a valid JSON string converted from a string (JSON_QUOTE) or any type of data (JSON_STRING).",
        "Note: JSON_QUOTE will escape interior quote and special characters (’\"’, ‘', ‘/’, ‘b’, ‘f’, ’n’, ‘r’, ’t’)"
}, examples = {
        "json_quote('Column1\\tColumn2) = \\\"Column1\\\\tColumn2\\\"",
        "json_string(true) = \"true\""
})
public class JsonQuoteFunction implements ValueParser {

    private ValueParser jsonParser;

    public JsonQuoteFunction(Function expr) {
        this.jsonParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (jsonParser == null) {
            return null;
        }
        Object parse = jsonParser.parse(sourceData, rowIndex, context);
        if (parse == null) {
            return null;
        }
        return JSON.toJSONString(parse);
    }
}
