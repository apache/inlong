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

/**
 * ArrayContainsFunction  ->  array_contains(haystack, needle)
 * description:
 * - Return NULL if the array itself is null
 * - Return whether the given element exists in an array.
 * Note: Checking for null elements in the array is supported.
 */
@TransformFunction(type = FunctionConstant.COLLECTION_TYPE, names = {
        "array_contains"}, parameter = "(Array array, Object needle)", descriptions = {
                "- Return \"\" if 'array' is NULL;", "- Return whether the given element exists in 'array'.",
                "Note: Checking for null elements in the array is supported."}, examples = {
                        "array_contains(array('he',7,'xxd'), 'cloud') = false",
                        "array_contains(array('he',-1,''),'') = true"})
public class ArrayContainsFunction implements ValueParser {

    private final ValueParser arrayParser;

    private final ValueParser elementParser;

    public ArrayContainsFunction(Function expr) {
        this.arrayParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
        this.elementParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object arrayObj = arrayParser.parse(sourceData, rowIndex, context);
        Object elementObj = elementParser.parse(sourceData, rowIndex, context);
        if (arrayObj == null) {
            return null;
        }
        if (arrayObj instanceof ArrayList) {
            ArrayList<?> array = (ArrayList<?>) arrayObj;
            if (array.isEmpty()) {
                return null;
            }
            for (Object element : array) {
                if (element == null && elementObj == null) {
                    return true;
                }
                if (element != null && element.equals(elementObj)) {
                    return true;
                }
            }
        }
        return false;
    }
}
