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
import org.apache.inlong.sdk.transform.process.pojo.IntervalInfo;
import org.apache.inlong.sdk.transform.process.utils.DateUtil;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

/**
 * DateAddFunction  ->  DATE_ADD(dateStr,INTERVAL intervalExprStr KEYWORD)
 * Description:
 * - Return NULL if dateStr is NULL.
 * - Return DATE if the date argument is a DATE value and your calculations involve only YEAR, MONTH, and DAY parts
 *          (that is, no time parts).
 * - Return TIME if the date argument is a TIME value and the calculations involve only HOURS, MINUTES,
 *          and SECONDS parts (that is, no date parts).
 * - Return DATETIME if the first argument is a DATETIME (or TIMESTAMP) value, or if the first argument is a DATE
 *          and the unit value uses HOURS, MINUTES, or SECONDS, or if the first argument is of type TIME and the
 *          unit value uses YEAR, MONTH, or DAY.
 * - Return String otherwise (type VARCHAR).
 * Note: Regarding the type of KEYWORD, please refer to the description of Interval on the MySQL official website.
 */
@TransformFunction(type = FunctionConstant.TEMPORAL_TYPE, names = {
        "date_add"}, parameter = "(String dateStr,String intervalExprStr)", descriptions = {
                "- Return \"\" if 'dateStr' is NULL;",
                "- Return DATE if 'dateStr' is a DATE value and your calculations involve only YEAR, MONTH, and DAY " +
                        "parts(that is, no time parts);",
                "- Return TIME if 'dateStr' is a TIME value and the calculations involve only HOURS, MINUTES,and SECONDS "
                        +
                        "parts (that is, no date parts);",
                "- Return DATETIME if the first argument is a DATETIME (or TIMESTAMP) value, or if the first argument is "
                        +
                        "a DATE and the unit value uses HOURS, MINUTES, or SECONDS, or if the first argument is of type TIME "
                        +
                        "and the unit value uses YEAR, MONTH, or DAY;",
                "- Return String otherwise (type VARCHAR).",
                "Note: Regarding intervalExpr, please refer to the MySQL official website."
        }, examples = {
                "date_add('2020-12-31 23:59:59',INTERVAL 999 DAY) = \"2023-09-26 23:59:59\"",
                "DATE_ADD('1992-12-31 23:59:59', INTERVAL '-1.999999' SECOND_MICROSECOND) = \"1992-12-31 23:59:57.000001\""
        })
public class DateAddFunction implements ValueParser {

    private final ValueParser datetimeParser;
    private final ValueParser intervalParser;

    public DateAddFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        datetimeParser = OperatorTools.buildParser(expressions.get(0));
        intervalParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object intervalInfoObj = intervalParser.parse(sourceData, rowIndex, context);
        Object dateObj = datetimeParser.parse(sourceData, rowIndex, context);
        if (intervalInfoObj == null || dateObj == null) {
            return null;
        }
        return DateUtil.dateTypeAdd(OperatorTools.parseString(dateObj),
                (IntervalInfo) intervalInfoObj, true);
    }
}
