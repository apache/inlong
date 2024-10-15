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
 * InsertFunction  ->  insert(str,pos,len,newStr)
 * description:
 * - Return NULL If any parameter is NULL
 * - Return the result of replacing the substring of length len with newStr starting from the given position pos in Str.
 * Note: If the position is out of the string's bounds, the original string is returned.If the length exceeds the
 *       remaining length of the string from the given position, the replacement continues to the end of the string.
 *       If any argument is null, the function returns null.
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "insert"}, parameter = "(String str,Integer pos,Integer len,String newStr)", descriptions = {
                "- Return \"\" If any parameter is NULL;",
                "- Return the result of replacing the substring of length len with 'newStr' starting from the given " +
                        "position 'pos' in 'str'.",
                "Note: If the position is out of the string's bounds, the original string is returned.If the length " +
                        "exceeds the remaining length of the string from the given position, the replacement continues "
                        +
                        "to the end of the string. If any argument is null, the function returns null."
        }, examples = {
                "INSERT('12345678', 3, 4, 'word') = '12word78'",
                "INSERT('12345678', -1, 4, 'word') = '12345678'",
                "INSERT('12345678', 3, 100, 'word') = '12word'"
        })
public class InsertFunction implements ValueParser {

    private ValueParser strParser;
    private ValueParser posParser;
    private ValueParser lenParser;
    private ValueParser newStrParser;

    public InsertFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        strParser = OperatorTools.buildParser(expressions.get(0));
        posParser = OperatorTools.buildParser(expressions.get(1));
        lenParser = OperatorTools.buildParser(expressions.get(2));
        newStrParser = OperatorTools.buildParser(expressions.get(3));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object strObject = strParser.parse(sourceData, rowIndex, context);
        Object posObject = posParser.parse(sourceData, rowIndex, context);
        Object lenObject = lenParser.parse(sourceData, rowIndex, context);
        Object newStrObject = newStrParser.parse(sourceData, rowIndex, context);

        if (strObject == null || posObject == null || lenObject == null || newStrObject == null) {
            return null;
        }

        String str = OperatorTools.parseString(strObject);
        int pos = OperatorTools.parseBigDecimal(posObject).intValue();
        int len = OperatorTools.parseBigDecimal(lenObject).intValue();
        String newStr = OperatorTools.parseString(newStrObject);

        if (str == null || newStr == null) {
            return null;
        }

        if (pos < 1 || pos > str.length()) {
            return str;
        }

        int startIndex = pos - 1;
        int endIndex = Math.min(startIndex + len, str.length());

        StringBuilder result = new StringBuilder();
        result.append(str, 0, startIndex);
        result.append(newStr);
        if (endIndex < str.length()) {
            result.append(str, endIndex, str.length());
        }

        return result.toString();
    }
}