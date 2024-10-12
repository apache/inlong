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
import java.util.List;

/**
 * ArrayRemoveFunction  ->  ARRAY_REMOVE(array, element)
 * description:
 * - Return NULL if 'array' is null;
 * - Return the result of removing all elements that equal to 'element' from 'array'.
 */
@TransformFunction(type = FunctionConstant.COLLECTION_TYPE, names = {
        "array_remove"}, parameter = "(Array array,Object element)", descriptions = {
                "- Return \"\" if 'array' is null;",
                "- Return the result of removing all elements that equal to 'element' from 'array'."}, examples = {
                        "array_remove(array('he',7,'xxd'),'he') = [7, xxd]"})
public class ArrayRemoveFunction implements ValueParser {

    private final ValueParser arrayParser;

    private ValueParser needleParser;

    public ArrayRemoveFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        this.arrayParser = OperatorTools.buildParser(expressions.get(0));
        if (expressions.size() > 1) {
            this.needleParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object arrayObj = arrayParser.parse(sourceData, rowIndex, context);
        Object needleObj = needleParser.parse(sourceData, rowIndex, context);
        if (arrayObj == null) {
            return null;
        }
        if (arrayObj instanceof ArrayList) {
            ArrayList<?> array = (ArrayList<?>) arrayObj;
            if (array.isEmpty()) {
                return null;
            }

            array.removeIf(element -> (element == null && needleObj == null)
                    || (element != null && element.equals(needleObj)));
            return array;
        }
        return null;
    }
}