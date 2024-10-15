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

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
/**
 * ElementFunction  ->  ELEMENT(array)
 * description:
 * - Return NULL if 'array' is empty or NULL;
 * - Return the sole element of 'array' (whose cardinality should be one).
 * Note: Throws an exception if array has more than one element.
 */
@Slf4j
@TransformFunction(type = FunctionConstant.COLLECTION_TYPE, names = {
        "element"}, parameter = "(Array array)", descriptions = {
                "- Return \"\" if 'array' is empty or NULL;",
                "- Return the sole element of 'array' (whose cardinality should be one).",
                "Note: Throws an exception if array has more than one element."
        }, examples = {"element(array('he')) = he"})
public class ElementFunction implements ValueParser {

    private final ValueParser valueParser;

    public ElementFunction(Function expr) {
        valueParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object valueObj = valueParser.parse(sourceData, rowIndex, context);
        if (valueObj instanceof ArrayList) {
            ArrayList<?> array = (ArrayList<?>) valueObj;
            if (array.isEmpty()) {
                return null;
            } else if (array.size() == 1) {
                return array.get(0);
            } else {
                log.warn("Array contains more than one element", new IllegalArgumentException());
                return null;
            }
        }
        return null;
    }

}
