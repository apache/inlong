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
import org.apache.inlong.sdk.transform.process.utils.DateUtil;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;

/**
 * DateDiffFunction  ->  datediff(dateStr1, dateStr2)
 * Description:
 * - Return null if one of the two parameters is null or ""
 * - Return null if one of the two parameters has an incorrect date format
 * - Return the number of days between the dates dateStr1->dateStr2.
 */
@TransformFunction(type = FunctionConstant.TEMPORAL_TYPE, names = {
        "datediff"}, parameter = "(String dateStr1, String dateStr2)", descriptions = {
                "- Return \"\" if one of the two parameters is null or \"\";",
                "- Return \"\" if one of the two parameters has an incorrect date format;",
                "- Return the number of days between the dates 'dateStr1'->'dateStr2'."}, examples = {
                        "datediff('2018-12-10 12:30:00', '2018-12-09 13:30:00') = 1",
                        "datediff('2018-12', '2018-12-12') = \"\""})
public class DateDiffFunction implements ValueParser {

    private final ValueParser leftDateParser;
    private final ValueParser rightDateParser;

    public DateDiffFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        leftDateParser = OperatorTools.buildParser(expressions.get(0));
        rightDateParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object leftDateObj = leftDateParser.parse(sourceData, rowIndex, context);
        Object rightDateObj = rightDateParser.parse(sourceData, rowIndex, context);
        if (leftDateObj == null || rightDateObj == null) {
            return null;
        }
        String leftDate = OperatorTools.parseString(leftDateObj);
        String rightDate = OperatorTools.parseString(rightDateObj);
        if (leftDate.isEmpty() || rightDate.isEmpty()) {
            return null;
        }
        try {
            LocalDate left = Objects.requireNonNull(DateUtil.parseLocalDateTime(leftDate)).toLocalDate();
            LocalDate right = Objects.requireNonNull(DateUtil.parseLocalDateTime(rightDate)).toLocalDate();
            return ChronoUnit.DAYS.between(right, left);
        } catch (Exception e) {
            return null;
        }
    }
}
