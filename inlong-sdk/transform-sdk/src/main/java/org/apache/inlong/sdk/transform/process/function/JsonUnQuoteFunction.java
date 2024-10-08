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
import com.alibaba.fastjson.JSONException;
import net.sf.jsqlparser.expression.Function;
/**
 * JsonUnQuoteFunction
 * description: JSON_UNQUOTE(string)--Unquotes JSON value, unescapes escaped special characters (’"’, ‘', ‘/’, ‘b’,
 *              ‘f’, ’n’, ‘r’, ’t’, ‘u’ hex hex hex hex), and returns the result as a string. If the argument is NULL,
 *              returns NULL. If the value does not start and end with double quotes or if it starts and ends with double
 *              quotes but is not a valid JSON string literal, the value is passed through unmodified.
 * for example: json_unquote('Hello, World!')--return "Hello, World!"
 *              json_unquote('Complex string with / and \\')--return "Complex string with / and \\"
 */
@TransformFunction(names = {"json_unquote"})
public class JsonUnQuoteFunction implements ValueParser {

    private ValueParser jsonParser;

    public JsonUnQuoteFunction(Function expr) {
        this.jsonParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (jsonParser == null) {
            return null;
        }
        String jsonString = OperatorTools.parseString(jsonParser.parse(sourceData, rowIndex, context));
        if (jsonString == null) {
            return null;
        }
        if (jsonString.length() < 2 || jsonString.charAt(0) != '"'
                || jsonString.charAt(jsonString.length() - 1) != '"') {
            return jsonString;
        }
        try {
            return JSON.parseObject(jsonString, String.class);
        } catch (JSONException e) {
            return jsonString;
        }
    }

}
