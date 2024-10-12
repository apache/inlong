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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * ToTimestampFunction  ->  to_timestamp(str[, format])
 * description:
 * - Return NULL if 'str' is NULL;
 * - Return the result of converting the date and time string 'str' to the 'format'
 *          (by default: yyyy-MM-dd HH:mm:ss if not specified) under the ‘UTC+0’ time zone to a timestamp.
 */
@TransformFunction(names = {"to_timestamp"}, parameter = "(String str [,String format])", descriptions = {
        "- Return \"\" if 'str' is NULL;",
        "- Return the result of converting the date and time string 'str' to the 'format' " +
                "(by default: yyyy-MM-dd HH:mm:ss if not specified) under the 'UTC+0' time zone to a timestamp."
}, examples = {
        "to_timestamp('1970/01/01 00:00:44', 'yyyy/MM/dd HH:mm:ss') = \"1970-01-01 00:00:44.0\""
})
public class ToTimestampFunction implements ValueParser {

    private ValueParser stringParser;
    private ValueParser formatParser;
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public ToTimestampFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();

        // Determine the number of arguments and build parser
        stringParser = OperatorTools.buildParser(expressions.get(0));
        if (expressions.size() == 2) {
            formatParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        String dateString = OperatorTools.parseString(stringParser.parse(sourceData, rowIndex, context));
        String formatPattern =
                formatParser != null ? OperatorTools.parseString(formatParser.parse(sourceData, rowIndex, context))
                        : DEFAULT_FORMAT;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(formatPattern);

        LocalDateTime dateTime;
        try {
            // Try parsing as LocalDateTime
            dateTime = LocalDateTime.parse(dateString, formatter);
        } catch (DateTimeParseException e) {
            // If LocalDateTime parsing fails, try parsing as LocalDate and use default LocalTime
            LocalDate date = LocalDate.parse(dateString, formatter);
            dateTime = LocalDateTime.of(date, LocalTime.MIDNIGHT);
        }

        // Convert LocalDateTime to Timestamp in UTC+0
        long utcMillis = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        Timestamp timestamp = new Timestamp(utcMillis);

        // Convert Timestamp to a string in UTC+0
        DateTimeFormatter outputFormatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S").withZone(ZoneId.of("UTC"));
        return outputFormatter.format(timestamp.toInstant());
    }
}