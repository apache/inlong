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

import net.sf.jsqlparser.expression.Function;

/**
 * IsDigitFunction  ->  is_digit(string)
 * description:
 * - return true if all characters in string are digit.
 * - return false otherwise (Including cases where string is null and '').
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "is_digit"}, parameter = "(String str)", descriptions = {
                "- Return \"\" If 'str' is NULL;", "- Return true if all characters in 'str' are digit;",
                "- Return false otherwise (Including cases where 'str' is null and '')."}, examples = {
                        "is_digit('3.5') = false"})
public class IsDigitFunction implements ValueParser {

    private final ValueParser stringParser;

    public IsDigitFunction(Function expr) {
        stringParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObject = stringParser.parse(sourceData, rowIndex, context);
        if (stringObject == null) {
            return false;
        }
        String string = OperatorTools.parseString(stringObject).toLowerCase();
        if (string.isEmpty()) {
            return false;
        }
        for (char chr : string.toCharArray()) {
            if (chr < '0' || chr > '9') {
                return false;
            }
        }
        return true;
    }
}
