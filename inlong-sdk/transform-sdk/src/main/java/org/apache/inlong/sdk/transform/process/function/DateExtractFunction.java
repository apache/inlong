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
 * - year(date)--returns the year from SQL date date
 * - quarter(date)--returns the quarter of a year (an integer between 1 and 4) from SQL date date
 * - month(date)--returns the month of a year (an integer between 1 and 12) from SQL date date
 * - week(date)--returns the week of a year (an integer between 1 and 53) from SQL date date
 * - dayofyear(date)--returns the day of a year (an integer between 1 and 366) from SQL date date
 * - dayofmonth(date)--returns the day of a month (an integer between 1 and 31) from SQL date date
 */
public class DateExtractFunction implements ValueParser {

    private int type;
    private ValueParser dateParser;
    private static final TemporalField weekOfYearField = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();

    /**
     * Constructor
     *
     * @param expr
     */
    public DateExtractFunction(int type, Function expr) {
        this.type = type;
        List<Expression> expressions = expr.getParameters().getExpressions();
        dateParser = OperatorTools.buildParser(expressions.get(0));
    }

    /**
     * parse
     *
     * @param sourceData
     * @param rowIndex
     * @return
     */
    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object dateObj = dateParser.parse(sourceData, rowIndex, context);
        Date date = OperatorTools.parseDate(dateObj);
        LocalDate localDate = date.toLocalDate();
        switch (type) {
            // year
            case 1:
                return localDate.getYear();
            // quarter(between 1 and 4)
            case 2:
                return (localDate.getMonthValue() - 1) / 3 + 1;
            // month(between 1 and 12)
            case 3:
                return localDate.getMonthValue();
            // week(between 1 and 53)
            case 4:
                return localDate.get(weekOfYearField);
            // dayofyear(between 1 and 366)
            case 5:
                return localDate.getDayOfYear();
            // dayofmonth(between 1 and 31)
            case 6:
                return localDate.getDayOfMonth();
            default:
                return null;
        }
    }
}
