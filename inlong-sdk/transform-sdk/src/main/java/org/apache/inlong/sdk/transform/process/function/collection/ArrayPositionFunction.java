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
 * ArrayPositionFunction  ->  ARRAY_POSITION(array, element)
 * description:
 * - Return NULL if 'array' is null;
 * - Return the position of the first occurrence of 'element' in the given 'array' as int;
 * - Return 0 if the given value could not be found in the 'array'.
 */
@TransformFunction(type = FunctionConstant.COLLECTION_TYPE, names = {
        "array_position"}, parameter = "(Array array,Object element)", descriptions = {
                "- Return \"\" if any parameter is null;",
                "- Return the position of the first occurrence of 'element' in the given 'array' as int (starts from 1);",
                "- Return 0 if the given value could not be found in the 'array'."
        }, examples = {
                "array_position(array('he',7,'xxd'),'he') = 1",
                "array_position(array('he',7,''),'_') = 0"
        })
public class ArrayPositionFunction implements ValueParser {

    private final ValueParser arrayParser;

    private final ValueParser needleParser;

    public ArrayPositionFunction(Function expr) {
        this.arrayParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));;
        this.needleParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(1));;
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object arrayObj = arrayParser.parse(sourceData, rowIndex, context);
        Object needleObj = needleParser.parse(sourceData, rowIndex, context);
        if (arrayObj == null || needleObj == null) {
            return null;
        }
        if (arrayObj instanceof ArrayList) {
            ArrayList<?> array = (ArrayList<?>) arrayObj;
            if (array.isEmpty()) {
                return null;
            }
            for (int i = 0; i < array.size(); i++) {
                if (array.get(i) == null) {
                    continue;
                }
                if (array.get(i).equals(needleObj)) {
                    return i + 1;
                }
            }

            return 0;
        }
        return null;
    }
}
