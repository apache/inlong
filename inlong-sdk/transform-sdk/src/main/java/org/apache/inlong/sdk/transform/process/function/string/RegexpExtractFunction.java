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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RegexpExtractFunction  ->  REGEXP_EXTRACT(str, regexp [, extractIndex])
 * description:
 * - Return NULL if any of the arguments are NULL or invalid
 * - Return a string from 'str' which extracted with a specified regexp expression 'regexp' and a regexp match group
 * index 'extractIndex'.
 * Note: 'regexp' must be a Java regular expression. 'extractIndex' indicates which regexp group to extract and starts
 * from 1, also the default value if not specified. 0 means matching the entire 'regexp' expression.In addition,
 * the regexp match group index should not exceed the number of the defined groups
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "regexp_extract"}, parameter = "(String str, String regexp, [Integer extractIndex])", descriptions = {
                "- Return \"\" if any of the arguments are NULL or invalid;",
                "- Return a string from 'str' which extracted with a specified regexp expression 'regexp' and a regexp match group index 'extractIndex'.",
                "Note: 'regexp' must be a Java regular expression. 'extractIndex' indicates which regexp group to extract and starts from 1, also the default value if not specified. 0 means matching the entire 'regexp' expression. In addition, the regexp match group index should not exceed the number of the defined groups."}, examples = {
                        "REGEXP_EXTRACT(\"abc123def\", \"(\\\\d+)\", 1) = 123",
                        "REGEXP_EXTRACT(\"Name: John, Age: 25, Location: NY\", \"Name: (\\\\w+), Age: (\\\\d+), Location: (\\\\w+)\", 2) = 25",
                        "REGEXP_EXTRACT(\"abc123def\", \"(\\\\d+)\", 2) = null",
                        "REGEXP_EXTRACT(\"abc123def\", \"abcdef\", 1) = null"})
public class RegexpExtractFunction implements ValueParser {

    private ValueParser inputStringParser;

    private ValueParser patternStringParser;

    private ValueParser indexIntegerParser;

    public RegexpExtractFunction(Function expr) {
        if (expr.getParameters() != null) {
            List<Expression> expressions = expr.getParameters().getExpressions();
            if (expressions != null && expressions.size() >= 3) {
                inputStringParser = OperatorTools.buildParser(expressions.get(0));
                patternStringParser = OperatorTools.buildParser(expressions.get(1));
                indexIntegerParser = OperatorTools.buildParser(expressions.get(2));
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (inputStringParser == null || patternStringParser == null || indexIntegerParser == null) {
            return null;
        }
        String inputString = OperatorTools.parseString(inputStringParser.parse(sourceData, rowIndex, context));
        String patternString = OperatorTools.parseString(patternStringParser.parse(sourceData, rowIndex, context));
        int indexInteger =
                OperatorTools.parseBigDecimal(indexIntegerParser.parse(sourceData, rowIndex, context)).intValue();
        if (indexInteger < 0) {
            return null;
        }
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(inputString);
        if (matcher.find()) {
            if (indexInteger <= matcher.groupCount()) {
                return matcher.group(indexInteger);
            }
        }
        return null;
    }
}
