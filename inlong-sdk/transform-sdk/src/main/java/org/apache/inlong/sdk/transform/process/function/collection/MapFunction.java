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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * MapFunction  ->  MAP(value1, value2, ...)
 * description:
 * - Return NULL if the number of parameters is not even
 * - Return a map created from a list of key-value pairs ((value1, value2), ... )
 */
@TransformFunction(type = FunctionConstant.COLLECTION_TYPE, names = {
        "map"}, parameter = "([String value1, String value2, ...])", descriptions = {
                "- Return \"\" if the number of parameters is not even;",
                "- Return a map created from a list of key-value pairs ((value1, value2), ... )."}, examples = {
                        "Map('he',7,'xxd') = null", "Map('he',1,'xxd','cloud') = {he=1, xxd=cloud}",
                        "Map('xxd','cloud',map(1,2),map(3,'apple')) = {xxd=cloud, {1=2}={3=apple}}"})
public class MapFunction implements ValueParser {

    private List<ValueParser> parserList;

    public MapFunction(Function expr) {
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
        if (parserList.size() % 2 != 0) {
            throw new IllegalArgumentException("Input values must be in key-value pairs.");
        }
        Map<Object, Object> res = new LinkedHashMap<>();

        for (int i = 0; i < parserList.size(); i += 2) {
            Object key = parserList.get(i).parse(sourceData, rowIndex, context);
            Object value = parserList.get(i + 1).parse(sourceData, rowIndex, context);
            res.put(key, value);
        }
        return res;
    }

}
