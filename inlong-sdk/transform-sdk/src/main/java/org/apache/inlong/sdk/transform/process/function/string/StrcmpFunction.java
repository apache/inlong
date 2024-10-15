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
 * StrcmpFunction  ->  strcmp(s1,s2)
 * description:
 * - Return NULL if either argument is NULL
 * - Return 0 if the strings are the same
 * - Return -1 if the first argument is smaller than the second according to the current sort order
 * - Return 1 otherwise
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "strcmp"}, parameter = "(String s1, String s2)", descriptions = {
                "- Return \"\" if either argument is NULL;",
                "- Return 0 if the strings are the same;",
                "- Return -1 if 's1' is smaller than 's2' according to the current sort order;",
                "- Return 1 otherwise."
        }, examples = {"strcmp('hello world','banana') = 1"})
public class StrcmpFunction implements ValueParser {

    private final ValueParser leftStringParser;
    private final ValueParser rightStringParser;

    public StrcmpFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        leftStringParser = OperatorTools.buildParser(expressions.get(0));
        rightStringParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object leftStringObj = leftStringParser.parse(sourceData, rowIndex, context);
        Object rightStringObj = rightStringParser.parse(sourceData, rowIndex, context);
        if (leftStringObj == null || rightStringObj == null) {
            return null;
        }
        String leftString = OperatorTools.parseString(leftStringObj);
        String rightString = OperatorTools.parseString(rightStringObj);
        int cmp = OperatorTools.compareValue(leftString, rightString);
        if (cmp > 0) {
            return 1;
        } else if (cmp < 0) {
            return -1;
        }
        return 0;
    }
}
