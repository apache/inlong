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
 * LpadFunction  ->  Lpad(str1,len,str2)
 * description:  Fill string s2 at the beginning of string s1 to make the string length len
 * - Return null if any of the three parameters is null or len is less than 0
 * - Return the substring of s1 with subscripts in the range of [0, len) if len is less than or equal to the length of s1
 * - if s2 is ""
 *      - Return "" if len is longer than the length of s1
 * - if s2 is not ""
 *      - Return the filled string
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "lpad"}, parameter = "(String str1, Integer len, String str2)", descriptions = {
                "- Return null if any of the three parameters is null or 'len' is less than 0;",
                "- Return the substring of 'str1' with subscripts in the range of [0, 'len') if 'len' is less than or equal to the length of 'str1';",
                "- Return \"\" if 'len' is longer than the length of 'str1' and 'str2' is \"\";",
                "- Return the filled string if 'str2' is not \"\"."}, examples = {"lpad('he',7,'xxd') = \"xxdxxhe\"",
                        "lpad('he',1,'') = \"h\""})
public class LpadFunction implements ValueParser {

    private final ValueParser leftStringParser;
    private final ValueParser lengthParser;
    private final ValueParser rightStringParser;

    public LpadFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        leftStringParser = OperatorTools.buildParser(expressions.get(0));
        lengthParser = OperatorTools.buildParser(expressions.get(1));
        rightStringParser = OperatorTools.buildParser(expressions.get(2));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object leftStringObj = leftStringParser.parse(sourceData, rowIndex, context);
        Object lengthObj = lengthParser.parse(sourceData, rowIndex, context);
        Object rightStringObj = rightStringParser.parse(sourceData, rowIndex, context);
        if (leftStringObj == null || lengthObj == null || rightStringObj == null) {
            return null;
        }
        int len = Integer.parseInt(OperatorTools.parseString(lengthObj));
        if (len < 0) {
            return null;
        }
        String leftStr = OperatorTools.parseString(leftStringObj);
        if (len <= leftStr.length()) {
            return leftStr.substring(0, len);
        }
        String rightStr = OperatorTools.parseString(rightStringObj);
        if (rightStr.isEmpty()) {
            return "";
        }
        int padLen = len - leftStr.length();
        StringBuilder builder = new StringBuilder(padLen);
        while (builder.length() < padLen) {
            builder.append(rightStr);
        }
        return builder.substring(0, padLen).concat(leftStr);
    }
}
