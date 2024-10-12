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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * RegexpMatchesFunction  ->  REGEX_MATCHES(source_string, regexp [, flags])
 * description:
 * - Return NULL if any of the arguments are NULL or invalid
 * - Return the result of the first match of the specified regular expression 'regexp' from 'source_string'
 * Note: 'flags' is one of  ('g' flag can be used when we want to match all the substrings that occur,
 *                 'i' flag to ignore case for matching),
 *                 'x' flag to extend syntax (ignoring whitespace and comments in regular expressions),
 *                 'm' and 'n' flag allows regular expressions to match across multiple lines)
 */
@TransformFunction(type = FunctionConstant.STRING_TYPE, names = {
        "regexp_matches"}, parameter = "(String source_string, String regexp [,String flags])", descriptions = {
                "- Return \"\" if any of the arguments are NULL or invalid;",
                "- Return the result of the first match of the specified regular expression 'regexp' from 'source_string'. Note: 'flags' is one of  ('g' -> flag can be used when we want to match all the substrings that occur, 'i' -> flag to ignore case for matching, 'x' -> flag to extend syntax (ignoring whitespace and comments in regular expressions), 'm' and 'n' -> flag allows regular expressions to match across multiple lines)"}, examples = {
                        "regexp_matches(\"The quick brown fox\", \"quick\") = [{\"quick\"}]",
                        "regexp_matches(\"foo 123 bar 456\", \"\\\\d+\", \"g\") = [{\"123\"},{\"456\"}]"})
public class RegexpMatchesFunction implements ValueParser {

    private ValueParser inputParser;

    private ValueParser patternParser;

    private ValueParser flagParser;

    public RegexpMatchesFunction(Function expr) {
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
        String inputString = OperatorTools.parseString(inputParser.parse(sourceData, rowIndex, context));
        String patternString = OperatorTools.parseString(patternParser.parse(sourceData, rowIndex, context));
        String flagString = "";
        if (flagParser != null) {
            flagString = OperatorTools.parseString(flagParser.parse(sourceData, rowIndex, context));
        }
        return regexpMatches(inputString, patternString, flagString);
    }

    private String regexpMatches(String input, String regex, String flags) {
        int flag = 0;
        if (flags != null) {
            if (flags.contains("i")) {
                flag |= Pattern.CASE_INSENSITIVE;
            }
            if (flags.contains("m") || flags.contains("n")) {
                flag |= Pattern.MULTILINE;
            }
            if (flags.contains("s")) {
                flag |= Pattern.DOTALL;
            }
            if (flags.contains("x")) {
                flag |= Pattern.COMMENTS;
            }
        }

        Pattern pattern = Pattern.compile(regex, flag);
        Matcher matcher = pattern.matcher(input);
        boolean isGlobalMatch = flags != null && flags.contains("g");
        List<String[]> matches = new ArrayList<>();

        while (matcher.find()) {
            int groupCount = matcher.groupCount();
            String[] matchGroups = new String[groupCount > 0 ? groupCount : 1];

            for (int i = 0; i <= groupCount; i++) {
                matchGroups[i == 0 ? 0 : i - 1] = matcher.group(i) != null ? matcher.group(i) : "";
            }
            matches.add(matchGroups);

            if (!isGlobalMatch) {
                break;
            }
        }
        return listToString(matches);
    }

    private String listToString(List<String[]> listOfArrays) {
        return listOfArrays.stream()
                .map(array -> Arrays.stream(array)
                        .map(s -> "\"" + s + "\"")
                        .collect(Collectors.joining(",", "{", "}")))
                .collect(Collectors.joining(",", "[", "]"));
    }

}
