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

import com.alibaba.fastjson.JSONPath;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;
/**
 * JsonQueryFunction
 * description: JSON_QUERY(jsonValue, path)--Extracts JSON values from a JSON string.
 * for example: json_query({\"people\": [{\"name\": \"Alice\"}, {\"name\": \"Bob\"}]}, $.people)
 *              --return [{"name":"Alice"},{"name":"Bob"}]
 *              json_query({\"list\": [null, {\"name\": \"John\"}]}, $.list[1].name)
 *              --return John
 */
@TransformFunction(names = {"json_query"})
public class JsonQueryFunction implements ValueParser {

    private final ValueParser jsonParser;

    private final ValueParser pathParser;

    public JsonQueryFunction(Function expr) {
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
        return res != null ? res.toString() : null;
    }
}
