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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;
/**
 * JsonArraysFunction
 * description: JSON_ARRAYS()--Builds a JSON array string from a list of values. This function returns a JSON string.
 *              The values can be arbitrary expressions.
 * for example: JSON_ARRAYS()--'[]'
 *              JSON_ARRAYS(1, '2')--'[1,"2"]'
 *              JSON_ARRAYS(JSON_ARRAY(1))--'[[1]]'
 */
@TransformFunction(names = {"json_arrays"})
public class JsonArraysFunction implements ValueParser {

    private List<ValueParser> parserList;

    public JsonArraysFunction(Function expr) {
        if (expr.getParameters() == null) {
            this.parserList = new ArrayList<>();
        } else {
            List<Expression> params = expr.getParameters().getExpressions();
            parserList = new ArrayList<>(params.size());
            for (Expression param : params) {
                ValueParser node = OperatorTools.buildParser(param);
                parserList.add(node);
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        JSONArray jsonArray = new JSONArray();
        for (ValueParser valueParser : parserList) {

            Object parseObj = valueParser.parse(sourceData, rowIndex, context);
            if (parseObj instanceof String && isJsonArray((String) parseObj)) {
                jsonArray.add(JSON.parseArray((String) parseObj));
            } else {
                jsonArray.add(parseObj);
            }
        }
        return jsonArray.toJSONString();
    }

    private boolean isJsonArray(String jsonStr) {
        try {
            JSON.parseArray(jsonStr);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
