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
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
/**
 * MapFromArraysFunction
 * description: MAP_FROM_ARRAYS(array_of_keys, array_of_values)--Returns a map created from an arrays of keys and values.
 *              Note that the lengths of two arrays should be the same.
 * for example: map_from_arrays(array('he', 'xxd'),array(1, 3))--return {he=1, xxd=3}
 *              map_from_arrays(array('xxd', array('cloud')),array(1, array(2)))--return {1=xxd, [2]=[cloud]}
 */
@Slf4j
@TransformFunction(names = {"map_from_arrays"})
public class MapFromArraysFunction implements ValueParser {

    private ValueParser keyArrayParser;

    private ValueParser valueArrayParser;

    public MapFromArraysFunction(Function expr) {
        if (expr.getParameters().getExpressions().size() >= 2) {
            this.keyArrayParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
            this.valueArrayParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object keyArrayObj = keyArrayParser.parse(sourceData, rowIndex, context);
        Object valueArrayObj = valueArrayParser.parse(sourceData, rowIndex, context);
        if (keyArrayObj == null || valueArrayObj == null) {
            return null;
        }
        if (keyArrayObj instanceof ArrayList && valueArrayObj instanceof ArrayList) {
            ArrayList<?> keyArray = ((ArrayList<?>) keyArrayObj);
            ArrayList<?> valueArray = ((ArrayList<?>) valueArrayObj);

            if (keyArray.size() != valueArray.size()) {
                log.warn("The lengths of the keys and values arrays must be the same.");
                return null;
            }
            Map<Object, Object> res = new LinkedHashMap<>();

            for (int i = 0; i < keyArray.size(); i++) {
                res.put(keyArray.get(i), valueArray.get(i));
            }
            return res;
        }
        return null;
    }
}
