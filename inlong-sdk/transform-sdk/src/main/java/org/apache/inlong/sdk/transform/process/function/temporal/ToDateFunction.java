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

package org.apache.inlong.sdk.transform.process.function.temporal;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ToDateFunction  ->  to_date(str[, format])
 * description:
 * - Return NULL if 'str' is NULL;
 * - Return the result of converting the date string 'str' to a date in the format 'format'(default is 'yyyy-MM-dd') .
 */
@TransformFunction(type = FunctionConstant.TEMPORAL_TYPE, names = {
        "to_date"}, parameter = "(String str [,String format])", descriptions = {
                "- Return \"\" if 'str' is NULL;",
                "- Return the result of converting the date string 'str' to a date in the format 'format'(default is 'yyyy-MM-dd')."
        }, examples = {"to_date('20240815', 'yyyyMMdd') = \"2024-08-15\""})
public class ToDateFunction implements ValueParser {

    private ValueParser stringParser1;
    private ValueParser stringParser2;
    private static final Map<String, DateTimeFormatter> INPUT_FORMATTERS = new ConcurrentHashMap<>();
    private static final DateTimeFormatter OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public ToDateFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        // Determine the number of arguments and build parser
        stringParser1 = OperatorTools.buildParser(expressions.get(0));
        if (expressions.size() == 2) {
            stringParser2 = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObj1 = stringParser1.parse(sourceData, rowIndex, context);
        String str1 = OperatorTools.parseString(stringObj1);
        String str2 = "yyyy-MM-dd";
        if (stringParser2 != null) {
            Object stringObj2 = stringParser2.parse(sourceData, rowIndex, context);
            str2 = OperatorTools.parseString(stringObj2);
        }
        LocalDate date = LocalDate.parse(str1, getDateTimeFormatter(str2));
        return date.format(OUTPUT_FORMATTER);
    }

    private DateTimeFormatter getDateTimeFormatter(String pattern) {
        DateTimeFormatter formatter = INPUT_FORMATTERS.get(pattern);
        if (formatter == null) {
            formatter = DateTimeFormatter.ofPattern(pattern);
            INPUT_FORMATTERS.put(pattern, formatter);
        }
        return formatter;
    }
}
