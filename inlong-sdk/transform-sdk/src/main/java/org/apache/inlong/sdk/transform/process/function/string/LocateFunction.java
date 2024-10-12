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

package org.apache.inlong.sdk.transform.process.function.string;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

/**
 * LocateFunction  ->  locate(string1, string2[, integer])
 * description:
 * - Return NULL if any of arguments is NULL
 * - Return 0 if not found
 * - Return the position of the first occurrence of string1 in string2 after position integer
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {"locate",
        "instr"}, parameter = "(String str1, String str2, Integer pos)", descriptions = {
                "- Return \"\" if any of arguments is NULL'", "- Return 0 if not found'",
                "- Return the position of the first occurrence of 'str1' in 'str2' after position 'pos'."}, examples = {
                        "locate('app', 'apple') = 1", "locate('app', 'appapp', 2) = 4"})
public class LocateFunction implements ValueParser {

    private ValueParser stringParser1;
    private ValueParser stringParser2;
    private ValueParser startPositionParser;

    public LocateFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        // Determine the number of arguments and build parser
        stringParser1 = OperatorTools.buildParser(expressions.get(0));
        stringParser2 = OperatorTools.buildParser(expressions.get(1));
        if (expressions.size() == 3) {
            startPositionParser = OperatorTools.buildParser(expressions.get(2));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObj1 = stringParser1.parse(sourceData, rowIndex, context);
        Object stringObj2 = stringParser2.parse(sourceData, rowIndex, context);
        // If any of arguments is null, return null
        if (stringObj1 == null || stringObj2 == null) {
            return null;
        }
        String str1 = OperatorTools.parseString(stringObj1);
        String str2 = OperatorTools.parseString(stringObj2);
        if (startPositionParser != null) {
            Object startPositionObj = startPositionParser.parse(sourceData, rowIndex, context);
            // if startPositionObj is null, return null
            if (startPositionObj == null) {
                return null;
            }
            int startPosition = OperatorTools.parseBigDecimal(startPositionObj).intValue();
            return str2.indexOf(str1, startPosition - 1) + 1;
        } else {
            return str2.indexOf(str1) + 1;
        }
    }
}
