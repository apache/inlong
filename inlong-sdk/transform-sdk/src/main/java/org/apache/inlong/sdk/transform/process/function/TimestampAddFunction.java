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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * TimestampAddFunction
 * Description: Add integer expression intervals to the date or date time expression expr.
 *  The unit of the time interval is specified by the unit parameter, which should be one of the following values:
 *  FRAC_SECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.
 */
@TransformFunction(names = {"timestamp_add", "timestampadd"})
public class TimestampAddFunction implements ValueParser {

    private ValueParser intervalParser;
    private ValueParser amountParser;
    private ValueParser datetimeParser;
    private static final DateTimeFormatter DEFAULT_FORMAT_DATE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter DEFAULT_FORMAT_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public TimestampAddFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        intervalParser = OperatorTools.buildParser(expressions.get(0));
        amountParser = OperatorTools.buildParser(expressions.get(1));
        datetimeParser = OperatorTools.buildParser(expressions.get(2));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        String interval = intervalParser.parse(sourceData, rowIndex, context).toString();
        Long amount = Long.parseLong(amountParser.parse(sourceData, rowIndex, context).toString());
        String dateString = datetimeParser.parse(sourceData, rowIndex, context).toString();
        return evalDate(dateString, interval, amount);
    }

    private String evalDate(String dateString, String interval, Long amount) {
        DateTimeFormatter formatter = null;
        LocalDateTime dateTime = null;
        boolean hasTime = true;
        if (dateString.indexOf(' ') != -1) {
            formatter = DEFAULT_FORMAT_DATE_TIME;
            dateTime = LocalDateTime.parse(dateString, formatter);
        } else {
            formatter = DEFAULT_FORMAT_DATE;
            dateTime = LocalDate.parse(dateString, formatter).atStartOfDay();
            hasTime = false;
        }

        switch (interval.toUpperCase()) {
            case "FRAC_SECOND":
                hasTime = true;
                dateTime = dateTime.plusNanos(amount * 1000_000);
                break;
            case "SECOND":
                hasTime = true;
                dateTime = dateTime.plusSeconds(amount);
                break;
            case "MINUTE":
                hasTime = true;
                dateTime = dateTime.plusMinutes(amount);
                break;
            case "HOUR":
                hasTime = true;
                dateTime = dateTime.plusHours(amount);
                break;
            case "DAY":
                dateTime = dateTime.plusDays(amount);
                break;
            case "WEEK":
                dateTime = dateTime.plusWeeks(amount);
                break;
            case "MONTH":
                dateTime = dateTime.plusMonths(amount);
                break;
            case "QUARTER":
                dateTime = dateTime.plusMonths(amount * 3);
                break;
            case "YEAR":
                dateTime = dateTime.plusYears(amount);
                break;
        }

        String result = dateTime.toLocalDate().toString();
        if (hasTime) {
            result += " " + dateTime.toLocalTime().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        }

        return result;
    }
}
