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
 * InitCapFunction  ->  initcap(str)
 * description:
 * - Return NULL if 'str' is NULL;
 * - Return a new form of STRING with the first character of each word converted to uppercase
 *          and the rest characters to lowercase.
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {"initcap",
        "init_cap"}, parameter = "(String s1, String s2)", descriptions = {"- Return \"\" if 'str' is NULL;",
                "- Return a new form of 'str' with the first character of each word converted to uppercase "
                        + "and the rest characters to lowercase."}, examples = {
                                "initcap('hello world') = \"Hello world\""})
public class InitCapFunction implements ValueParser {

    private ValueParser stringParser;

    public InitCapFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        stringParser = OperatorTools.buildParser(expressions.get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object strObject = stringParser.parse(sourceData, rowIndex, context);
        if (strObject == null) {
            return null;
        }
        String str = OperatorTools.parseString(strObject);
        if (str == null) {
            return null;
        }

        StringBuilder result = new StringBuilder();
        boolean capitalizeNext = true;
        for (char c : str.toCharArray()) {
            if (Character.isLetterOrDigit(c)) {
                if (capitalizeNext) {
                    result.append(Character.toUpperCase(c));
                    capitalizeNext = false;
                } else {
                    result.append(Character.toLowerCase(c));
                }
            } else {
                capitalizeNext = true;
                result.append(c);
            }
        }

        return result.toString();
    }
}
