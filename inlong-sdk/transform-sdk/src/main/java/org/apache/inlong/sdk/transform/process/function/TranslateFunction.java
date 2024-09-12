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

package org.apache.inlong.sdk.transform.process.function;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import com.google.common.collect.ImmutableMap;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TranslateFunction
 * translate(expression, find_chars, replace_chars)
 * Description:
 * For a given expression, replaces all occurrences of specified characters with specified substitutes.
 * Existing characters are mapped to replacement characters by their positions in the find_chars and replace_chars arguments.
 * If more characters are specified in the find_chars argument than in the replace_chars argument, the extra characters from the find_chars argument are omitted in the return value.
 *
 * Translate function is similar to the replace function and the regexp_replace function,
 * except that replace substitutes one entire string with another string and regexp_replace lets you search a string for a regular expression pattern,
 * while translate makes multiple single-character substitutions.
 *
 * Arguments:
 *      expression: The expression to be translated.
 *      find_chars: A string containing the characters to be replaced.
 *      replace_chars: A string containing the characters to substitute.
 * examples:
 *      case1: translate(email, '@', '.') -> original_expression: harry@inlong.com  target_expression: harry.inlong.com
 *      case2: translate(hello WorD, 'WD', 'wd') -> original_expression: hello WorD  target_expression: hello word
 */
@TransformFunction(names = {"translate"})
public class TranslateFunction implements ValueParser {

    private ValueParser originalStrParser;

    private ValueParser findCharsParser;

    private ValueParser replaceCharsParser;

    public TranslateFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        originalStrParser = OperatorTools.buildParser(expressions.get(0));
        findCharsParser = OperatorTools.buildParser(expressions.get(1));
        replaceCharsParser = OperatorTools.buildParser(expressions.get(2));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object originalStrObject = originalStrParser.parse(sourceData, rowIndex, context);
        Object findCharsObject = findCharsParser.parse(sourceData, rowIndex, context);
        Object replaceCharsObject = replaceCharsParser.parse(sourceData, rowIndex, context);
        String originalStr = OperatorTools.parseString(originalStrObject);
        String findChars = OperatorTools.parseString(findCharsObject);
        String replaceChars = OperatorTools.parseString(replaceCharsObject);

        if (originalStr == null) {
            return "";
        }
        StringBuilder builder = null;
        // Create a map to store character replacements
        Map<Character, Character> replacementMap = parseReplacementMap(findChars, replaceChars);

        for (int i = 0, size = originalStr.length(); i < size; i++) {
            char ch = originalStr.charAt(i);
            if (replacementMap.containsKey(ch)) {
                // Find the index of the current character in findChars,
                // and replace the character at that index with the character at the same index in replaceChars.
                if (builder == null) {
                    builder = new StringBuilder(size);
                    if (i > 0) {
                        builder.append(originalStr, 0, i);
                    }
                }
                ch = replacementMap.get(ch);
            }
            if (builder != null) {
                builder.append(ch);
            }
        }
        return builder == null ? originalStr : builder.toString();
    }

    private Map<Character, Character> parseReplacementMap(String findChars, String replaceChars) {
        if (StringUtils.isAnyBlank(findChars, replaceChars)) {
            return ImmutableMap.of();
        }

        final int findSize = findChars == null ? 0 : findChars.length();
        final int replaceSize = replaceChars == null ? 0 : replaceChars.length();
        final int commonSize = Math.min(findSize, replaceSize);
        // Create a map to store character replacements
        Map<Character, Character> replacementMap = new HashMap<>();
        for (int i = 0; i < commonSize; i++) {
            char findChar = findChars.charAt(i);
            char replaceChar = replaceChars.charAt(i);
            replacementMap.put(findChar, replaceChar);
        }
        return replacementMap;
    }
}
