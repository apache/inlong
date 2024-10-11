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
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;
import org.apache.inlong.sdk.transform.process.pojo.IntervalInfo;
import org.apache.inlong.sdk.transform.process.utils.DateUtil;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;

/**
 * DateAddFunction
 * Description:
 * - return NULL if date is NULL.
 * - return DATE if the date argument is a DATE value and your calculations involve only YEAR, MONTH, and DAY parts
 *          (that is, no time parts).
 * - return TIME if the date argument is a TIME value and the calculations involve only HOURS, MINUTES,
 *          and SECONDS parts (that is, no date parts).
 * - return DATETIME if the first argument is a DATETIME (or TIMESTAMP) value, or if the first argument is a DATE
 *          and the unit value uses HOURS, MINUTES, or SECONDS, or if the first argument is of type TIME and the
 *          unit value uses YEAR, MONTH, or DAY.
 * - return If the first argument is a dynamic parameter (for example, of a prepared statement), its resolved type
 *          is DATE if the second argument is an interval that contains some combination of YEAR, MONTH, or DAY values
 *          only; otherwise, its type is DATETIME.
 * - return String otherwise (type VARCHAR).
 */
@TransformFunction(names = {"date_sub", "datesub"})
public class DateSubFunction implements ValueParser {

    private final ValueParser datetimeParser;
    private final ValueParser intervalParser;

    public DateSubFunction(Function expr) {
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
                (IntervalInfo) intervalInfoObj, false);
    }
}
