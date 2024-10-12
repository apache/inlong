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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;
/**
 * ArrayJoinFunction  ->  ARRAY_JOIN(array, delimiter[, nullReplacement])
 * description:
 * - Return NULL if any parameter is null;
 * - Return a string indicating that the elements in the given 'array' are concatenated using 'delimiter',
 *        and empty array elements are filled with 'nullReplacement'.
 * Note: If nullReplacement is not specified, null elements in the array will be omitted from the resulting string.
 */
@TransformFunction(names = {
        "array_join"}, parameter = "(Array array, String delimiter[, String nullReplacement])", descriptions = {
                "- Return \"\" if any parameter is null;",
                "- Return a string indicating that the elements in the given 'array' are concatenated using a 'delimiter', "
                        +
                        "and empty array elements are filled with 'nullReplacement'.",
                "- Note: If nullReplacement is not specified, null elements in the array will be omitted from the resulting string."
        }, examples = {
                "array_join(array('he',7,'xxd'),'~') = he~7~xxd",
                "array_join(array('he',3,''),'~','oo') = he~3~oo"
        })
public class ArrayJoinFunction implements ValueParser {

    private ValueParser arrayParser;

    private ValueParser delimiterParser;

    private ValueParser nullReplacementParser;

    public ArrayJoinFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        if (expressions.size() >= 2) {
            this.arrayParser = OperatorTools.buildParser(expressions.get(0));
            this.delimiterParser = OperatorTools.buildParser(expressions.get(1));
            if (expressions.size() >= 3) {
                this.nullReplacementParser = OperatorTools.buildParser(expressions.get(2));
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object arrayObj = arrayParser.parse(sourceData, rowIndex, context);
        Object delimiterObj = delimiterParser.parse(sourceData, rowIndex, context);
        if (arrayObj == null || delimiterObj == null) {
            return null;
        }
        String delimiter = OperatorTools.parseString(delimiterObj);
        if (delimiter.isEmpty()) {
            return null;
        }
        String nullReplacement = parseNullReplacement(sourceData, rowIndex, context);

        if (arrayObj instanceof ArrayList) {
            return joinArrayWithDelimiter((ArrayList<?>) arrayObj, delimiter, nullReplacement);
        }

        return null;
    }

    private String joinArrayWithDelimiter(ArrayList<?> array, String delimiter, String nullReplacement) {
        StringBuilder result = new StringBuilder();

        for (int i = 0; i < array.size(); i++) {
            String element = (String) array.get(i);

            if (element == null || element.isEmpty()) {
                if (nullReplacement != null && !nullReplacement.isEmpty()) {
                    result.append(nullReplacement);
                }
            } else {
                result.append(element);
            }

            if (i < array.size() - 1) {
                result.append(delimiter);
            }
        }

        return result.toString();
    }

    private String parseNullReplacement(SourceData sourceData, int rowIndex, Context context) {
        if (nullReplacementParser != null) {
            Object nullReplacementObj = nullReplacementParser.parse(sourceData, rowIndex, context);
            return OperatorTools.parseString(nullReplacementObj);
        }
        return null;
    }
}
