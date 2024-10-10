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
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * RegexpSplitToArrayFunction
 * description: REGEXP_SPLIT_TO_ARRAY( string, pattern text|, flags text l) → text[]
 *              Splits string using a POslX regular expression as the delimiter, producing an array of results
 * parameters: 1) source_string: the string to be matched
 *             2) pattern: POSIX regular expression for matching
 *             3) flags: one or more characters that control the behavior of a function,
 *                'g' flag can be used when we want to match all the substrings that occur,
 *                'i' flag to ignore case for matching,
 *                'x' flag to extend syntax (ignoring whitespace and comments in regular expressions),
 *                'm' and 'n' flag allows regular expressions to match across multiple lines
 * for example: regexp_split_to_array("hello world","\s+")--return {hello, world}
 */
@TransformFunction(names = {"regexp_split_to_array"})
public class RegexpSplitToArrayFunction implements ValueParser {

    private ValueParser inputParser;

    private ValueParser patternParser;

    private ValueParser flagParser;

    public RegexpSplitToArrayFunction(Function expr) {
        if (expr.getParameters() != null) {
            List<Expression> expressions = expr.getParameters().getExpressions();
            if (expressions != null) {
                inputParser = OperatorTools.buildParser(expressions.get(0));
                patternParser = OperatorTools.buildParser(expressions.get(1));
                if (expressions.size() == 3) {
                    flagParser = OperatorTools.buildParser(expressions.get(2));
                }
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (inputParser == null || patternParser == null) {
            return null;
        }
        String inputString = OperatorTools.parseString(inputParser.parse(sourceData, rowIndex, context));
        String patternString = OperatorTools.parseString(patternParser.parse(sourceData, rowIndex, context));
        String flagString = "";
        if (flagParser != null) {
            flagString = OperatorTools.parseString(flagParser.parse(sourceData, rowIndex, context));
        }
        return regexpSplitToArray(inputString, patternString, flagString);
    }

    private List<String> regexpSplitToArray(String inputString, String patternString, String flagString) {
        int regexFlags = 0;

        if (flagString != null) {
            if (flagString.contains("i")) {
                regexFlags |= Pattern.CASE_INSENSITIVE;
            }
            if (flagString.contains("m") || flagString.contains("n")) {
                regexFlags |= Pattern.MULTILINE;
            }
            if (flagString.contains("s")) {
                regexFlags |= Pattern.DOTALL;
            }
            if (flagString.contains("x")) {
                regexFlags |= Pattern.COMMENTS;
            }
        }
        Pattern pattern = Pattern.compile(patternString, regexFlags);
        return Arrays.asList(pattern.split(inputString));
    }
}
