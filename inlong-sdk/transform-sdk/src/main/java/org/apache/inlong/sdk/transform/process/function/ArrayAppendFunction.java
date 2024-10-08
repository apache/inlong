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

import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
/**
 * ArrayAppendFunction
 * description: ARRAY_APPEND(array, element)--Appends an element to the end of the array and returns the result.
 *              If the array itself is null, the function will return null. If an element to add is null, the null
 *              element will be added to the end of the array.
 * for example: array_append(array('he',7,'xxd'), 'cloud')--return[he, 7, xxd, cloud]
 */
@TransformFunction(names = {"array_append"})
public class ArrayAppendFunction implements ValueParser {

    private final ValueParser arrayParser;

    private final ValueParser elementParser;

    public ArrayAppendFunction(Function expr) {
        this.arrayParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
        this.elementParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(1));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object arrayObj = arrayParser.parse(sourceData, rowIndex, context);
        Object elementObj = elementParser.parse(sourceData, rowIndex, context);
        if (arrayObj == null) {
            return null;
        }
        if (arrayObj instanceof ArrayList) {
            ArrayList<Object> array = (ArrayList<Object>) arrayObj;
            if (array.isEmpty()) {
                return null;
            }
            array.add(elementObj);
            return array;
        }
        return null;
    }
}
