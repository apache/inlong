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
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * ArrayConcatFunction  ->  ARRAY_CONCAT([array1, ...])
 * description:
 * - Return NULL if any input array is NULL;
 * - Return an array that is the result of concatenating at least one array.
 */
@TransformFunction(type = FunctionConstant.COLLECTION_TYPE, names = {
        "array_union"}, parameter = "(Array array1[, Array array2, ...])", descriptions = {
                "- Return \"\" if any input array is NULL;",
                "- Return an array that is the result of concatenating at least one array."}, examples = {
                        "array_concat(array('he',7),array('xxd', 'cloud')) = [he, 7, xxd, cloud]"})
public class ArrayUnionFunction implements ValueParser {

    private final ValueParser leftArrayParser;

    private final ValueParser rightArrayParser;

    public ArrayUnionFunction(Function expr) {
        this.leftArrayParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
        this.rightArrayParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object leftArrayObj = leftArrayParser.parse(sourceData, rowIndex, context);
        Object rightArrayObj = rightArrayParser.parse(sourceData, rowIndex, context);
        if (leftArrayObj == null || rightArrayObj == null) {
            return null;
        }
        if (leftArrayObj instanceof ArrayList && rightArrayObj instanceof ArrayList) {
            ArrayList<?> leftArray = (ArrayList<?>) leftArrayObj;
            ArrayList<?> rightArray = (ArrayList<?>) rightArrayObj;
            if (leftArray.isEmpty() || rightArray.isEmpty()) {
                return null;
            }

            Set<Object> res = new LinkedHashSet<>();
            res.addAll(leftArray);
            res.addAll(rightArray);

            return new ArrayList<>(res);
        }
        return null;
    }
}
