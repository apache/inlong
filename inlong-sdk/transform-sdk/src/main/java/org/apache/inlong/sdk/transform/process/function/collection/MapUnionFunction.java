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

package org.apache.inlong.sdk.transform.process.function.collection;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * MapUnionFunction  ->  MAP_UNION([map1, map2, ...])
 * description:
 * - Return NULL if any of maps is null
 * - Return a map created by merging at least one map.These maps should have a common map type,the following map will overwrite the previous one.
 */
@TransformFunction(type = FunctionConstant.COLLECTION_TYPE, names = {
        "map_union"}, parameter = "([Map map1, Map map2, ...])", descriptions = {
                "- Return \"\" if any of maps is null;",
                "- Return a map created by merging at least one map.These maps should have a common map type, "
                        + "the following map will overwrite the previous one."
        }, examples = {
                "map_union(map('he', 1),map('xxd', 3)) = {he=1, xxd=3}",
                "map_union(map('he', 1),map('he', 3)) = {he=3}"
        })
public class MapUnionFunction implements ValueParser {

    private List<ValueParser> parserList;

    public MapUnionFunction(Function expr) {
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
        Map<Object, Object> res = new HashMap<>();
        for (ValueParser valueParser : parserList) {
            Object mapObj = valueParser.parse(sourceData, rowIndex, context);
            if (mapObj == null) {
                return null;
            }
            if (mapObj instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) mapObj;
                if (map.isEmpty()) {
                    return null;
                }
                res.putAll(map);
            }
        }
        return res;
    }
}