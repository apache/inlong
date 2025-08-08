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

package org.apache.inlong.sdk.transform.process.operator;

import org.apache.inlong.common.util.StringUtil;
import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;

import java.util.regex.Pattern;

/**
 * LikeOperator
 * 
 */
@Slf4j
@TransformOperator(values = LikeExpression.class)
public class LikeOperator implements ExpressionOperator {

    private final ValueParser destParser;
    private final ValueParser patternParser;
    private final ValueParser escapeParser;
    private final boolean isNot;
    private static final String REGEX_SPECIAL_CHAR = "[]()|^-+*?{}$\\.";

    public LikeOperator(LikeExpression expr) {
        destParser = OperatorTools.buildParser(expr.getLeftExpression());
        patternParser = OperatorTools.buildParser(expr.getRightExpression());
        escapeParser = OperatorTools.buildParser(expr.getEscape());
        isNot = expr.isNot();
    }

    private String buildLikeRegex(String pattern, char escapeChar) {
        int len = pattern.length();
        StringBuilder regexPattern = new StringBuilder(len + len);
        for (int i = 0; i < len; i++) {
            char c = pattern.charAt(i);
            if (REGEX_SPECIAL_CHAR.indexOf(c) >= 0) {
                regexPattern.append('\\');
            }
            if (c == escapeChar) {
                if (i == (pattern.length() - 1)) {
                    // At the end of a string, the escape character represents itself
                    regexPattern.append(c);
                    continue;
                }
                char nextChar = pattern.charAt(i + 1);
                if (nextChar == '_' || nextChar == '%' || nextChar == escapeChar) {
                    regexPattern.append(nextChar);
                    i++;
                } else {
                    throw new RuntimeException("Illegal pattern string");
                }
            } else if (c == '_') {
                regexPattern.append('.');
            } else if (c == '%') {
                regexPattern.append("(?s:.*)");
            } else {
                regexPattern.append(c);
            }
        }
        return regexPattern.toString();
    }

    /**
     * check
     * @param sourceData
     * @param rowIndex
     * @return
     */
    @Override
    public boolean check(SourceData sourceData, int rowIndex, Context context) {
        Object destObj = destParser.parse(sourceData, rowIndex, context);
        Object patternObj = patternParser.parse(sourceData, rowIndex, context);
        if (destObj == null || patternObj == null) {
            return false;
        }
        char escapeChr = '\\';
        if (escapeParser != null) {
            Object escapeObj = this.escapeParser.parse(sourceData, rowIndex, context);
            if (!StringUtil.isEmpty(escapeObj)) {
                escapeChr = escapeObj.toString().charAt(0);
            }
        }
        String destStr = destObj.toString();
        String pattern = patternObj.toString();
        try {
            final String regex = buildLikeRegex(pattern, escapeChr);
            boolean isMatch = Pattern.matches(regex.toLowerCase(), destStr.toLowerCase());
            if (isNot) {
                return !isMatch;
            }
            return isMatch;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }
}
