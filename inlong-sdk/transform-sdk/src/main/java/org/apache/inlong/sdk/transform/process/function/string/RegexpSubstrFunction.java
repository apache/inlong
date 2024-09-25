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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RegexpSubstrFunction
 * description: REGEXP_SUBSTR(str, regexp)--Returns the first substring in str that matches regexp.
 *              Returns an STRING representation of the first matched substring. NULL if any of the arguments are NULL
 *              or regexp if invalid or pattern is not found.
 */
@TransformFunction(names = {"regex_substr"})
public class RegexpSubstrFunction implements ValueParser {

    private ValueParser inputStringParser;

    private ValueParser patternStringParser;

    public RegexpSubstrFunction(Function expr) {
        if (expr.getParameters() != null) {
            List<Expression> expressions = expr.getParameters().getExpressions();
            if (expressions != null && expressions.size() >= 2) {
                inputStringParser = OperatorTools.buildParser(expressions.get(0));
                patternStringParser = OperatorTools.buildParser(expressions.get(1));
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (inputStringParser == null || patternStringParser == null) {
            return null;
        }
        String inputString = OperatorTools.parseString(inputStringParser.parse(sourceData, rowIndex, context));
        String patternString = OperatorTools.parseString(patternStringParser.parse(sourceData, rowIndex, context));
        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(inputString);
        if (matcher.find()) {
            return matcher.group(0);
        } else {
            return null;
        }
    }
}
