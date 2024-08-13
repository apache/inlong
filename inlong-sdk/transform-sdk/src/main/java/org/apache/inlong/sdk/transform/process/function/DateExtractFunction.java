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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.sql.Date;
import java.time.LocalDate;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Locale;

/**
 * DateExtractFunction
 * description:
 * - year(date)--returns the year from SQL date
 * - quarter(date)--returns the quarter of a year (an integer between 1 and 4) from SQL date
 * - month(date)--returns the month of a year (an integer between 1 and 12) from SQL date
 * - week(date)--returns the week of a year (an integer between 1 and 53) from SQL date
 * - dayofyear(date)--returns the day of a year (an integer between 1 and 366) from SQL date
 * - dayofmonth(date)--returns the day of a month (an integer between 1 and 31) from SQL date
 */
public class DateExtractFunction implements ValueParser {

    private DateExtractFunctionType type;
    private ValueParser dateParser;
    private static final TemporalField weekOfYearField = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();

    public enum DateExtractFunctionType {
        YEAR, QUARTER, MONTH, WEEK, DAY_OF_YEAR, DAY_OF_MONTH
    }

    public DateExtractFunction(DateExtractFunctionType type, Function expr) {
        this.type = type;
        List<Expression> expressions = expr.getParameters().getExpressions();
        dateParser = OperatorTools.buildParser(expressions.get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object dateObj = dateParser.parse(sourceData, rowIndex, context);
        Date date = OperatorTools.parseDate(dateObj);
        LocalDate localDate = date.toLocalDate();
        switch (type) {
            // year
            case YEAR:
                return localDate.getYear();
            // quarter(between 1 and 4)
            case QUARTER:
                return (localDate.getMonthValue() - 1) / 3 + 1;
            // month(between 1 and 12)
            case MONTH:
                return localDate.getMonthValue();
            // week(between 1 and 53)
            case WEEK:
                return localDate.get(weekOfYearField);
            // dayofyear(between 1 and 366)
            case DAY_OF_YEAR:
                return localDate.getDayOfYear();
            // dayofmonth(between 1 and 31)
            case DAY_OF_MONTH:
                return localDate.getDayOfMonth();
            default:
                return null;
        }
    }
}
