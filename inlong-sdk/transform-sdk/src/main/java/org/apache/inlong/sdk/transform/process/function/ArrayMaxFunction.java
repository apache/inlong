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
 * ArrayMaxFunction
 * description: ARRAY_MAX(array)--Returns the maximum value from the array, if array itself is null
 *              , the function returns null.
 * for example: array_max(array(4,3,56))--return 56
 */
@TransformFunction(names = {"array_max"})
public class ArrayMaxFunction implements ValueParser {

    private final ValueParser arrayParser;

    public ArrayMaxFunction(Function expr) {
        this.arrayParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object arrayObj = arrayParser.parse(sourceData, rowIndex, context);
        if (arrayObj == null) {
            return null;
        }
        if (arrayObj instanceof ArrayList) {
            ArrayList<Integer> array = (ArrayList<Integer>) arrayObj;
            if (array.isEmpty()) {
                return null;
            }
            int max = Integer.parseInt(String.valueOf(array.get(0)));
            for (Object element : array) {
                int elementInt = Integer.parseInt((String) element);
                if (elementInt - max > 0) {
                    max = elementInt;
                }
            }
            return max;
        }
        return null;
    }
}
