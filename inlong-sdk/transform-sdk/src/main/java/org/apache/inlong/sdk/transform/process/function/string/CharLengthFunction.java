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
 * CharLengthFunction  ->  char_length(string)
 * Description:
 * - Return NULL if the string is NULL
 * - Return the character length of the string
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "char_length"}, parameter = "(String str)", descriptions = {
                "- Return \"\" if 'str' is NULL;",
                "- Return the character length of 'str'."
        }, examples = {
                "char_length('hello world') = 11",
                "case2: char_length('应龙') = 2"
        })
public class CharLengthFunction implements ValueParser {

    private final ValueParser stringParser;

    public CharLengthFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        stringParser = OperatorTools.buildParser(expressions.get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObject = stringParser.parse(sourceData, rowIndex, context);
        if (stringObject == null) {
            return null;
        }
        String str = OperatorTools.parseString(stringObject);
        return str.length();
    }
}
