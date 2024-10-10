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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;
/**
 * ArrayPrependFunction
 * description: ARRAY_PREPEND(array, element)--Appends an element to the beginning of the array and returns the result.
 *              If the array itself is null, the function will return null. If an element to add is null, the null
 *              element will be added to the beginning of the array.
 * for example: array_prepend(array(4,3),3)--return [3, 4, 3]
 */
@TransformFunction(names = {"array_prepend"})
public class ArrayPrependFunction implements ValueParser {

    private final ValueParser arrayParser;

    private ValueParser elementParser;

    public ArrayPrependFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        this.arrayParser = OperatorTools.buildParser(expressions.get(0));
        if (expressions.size() > 1) {
            this.elementParser = OperatorTools.buildParser(expressions.get(1));
        }
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

            List<Object> result = new ArrayList<>(array.size() + 1);

            result.add(elementObj);
            result.addAll(array);
            return result;
        }
        return null;
    }
}
