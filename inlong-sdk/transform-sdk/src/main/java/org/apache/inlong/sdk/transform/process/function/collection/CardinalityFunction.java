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

import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.Map;
/**
 * CardinalityFunction  ->  CARDINALITY(array) or CARDINALITY(map)
 * description:
 * - Return NULL if the input is NULL;
 * - Return the number of elements in array if the input is array.
 * - Return the number of entries in map if the input is map.
 */
@TransformFunction(type = FunctionConstant.COLLECTION_TYPE, names = {
        "cardinality"}, parameter = "(Object input)", descriptions = {
                "- Return \"\" if the input is NULL;",
                "- Return the number of elements in array if the input is array;",
                "- Return the number of entries in map if the input is map."}, examples = {
                        "cardinality(array('he',7,'xxd')) = 3", "cardinality(map('he',7,'xxd',3)) = 2"})
public class CardinalityFunction implements ValueParser {

    private final ValueParser valueParser;

    public CardinalityFunction(Function expr) {
        valueParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object valueObj = valueParser.parse(sourceData, rowIndex, context);
        if (valueObj instanceof ArrayList) {
            return ((ArrayList<?>) valueObj).size();

        } else if (valueObj instanceof Map) {
            return ((Map<?, ?>) valueObj).size();
        }
        return null;
    }
}
