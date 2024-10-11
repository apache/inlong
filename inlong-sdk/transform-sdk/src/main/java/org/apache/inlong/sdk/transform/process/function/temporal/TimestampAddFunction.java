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
import org.apache.inlong.sdk.transform.process.utils.DateUtil;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.time.LocalDateTime;
import java.util.List;

/**
 * TimestampAddFunction
 * Description: Add integer expression intervals to the date or date time expression expr.
 *  The unit of the time interval is specified by the unit parameter, which should be one of the following values:
 *  MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.
 */
@TransformFunction(names = {"timestamp_add", "timestampadd"})
public class TimestampAddFunction implements ValueParser {

    private final ValueParser intervalParser;
    private final ValueParser amountParser;
    private final ValueParser datetimeParser;

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
        LocalDateTime dateTime = DateUtil.parseLocalDateTime(dateString);
        if (dateTime == null) {
            return null;
        }
        boolean hasTime = dateString.indexOf(' ') != -1;
        boolean hasMicro = dateString.indexOf('.') != -1;

        switch (interval.toUpperCase()) {
            case "MICROSECOND":
                hasTime = true;
                hasMicro = true;
                dateTime = dateTime.plusNanos(amount * 1000);
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
        StringBuilder format = new StringBuilder("yyyy-MM-dd");
        if (hasTime) {
            format.append(" HH:mm:ss");
        }
        if (hasMicro) {
            format.append(".SSSSSS");
        }
        return dateTime.format(DateUtil.getDateTimeFormatter(format.toString()));
    }
}
