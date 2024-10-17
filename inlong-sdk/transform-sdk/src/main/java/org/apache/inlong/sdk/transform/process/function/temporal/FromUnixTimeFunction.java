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

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FromUnixTimeFunction  ->  form_unixtime(numeric[, formatStr])
 * description:
 * - Return NULL if numeric is NULL
 * - Return a representation of the numeric argument as a value in string
 * Note: formatStr defaults to 'yyyy-MM-dd HH:mm:ss'. numeric is an internal timestamp value representing seconds
 *       since ‘1970-01-01 00:00:00’ UTC, such as produced by the UNIX_TIMESTAMP() function.
 */
@TransformFunction(type = FunctionConstant.TEMPORAL_TYPE, names = {
        "from_unix_time",
        "form_unixtime"
}, parameter = "(Numeric numeric,String formatStr)", descriptions = {
        "- Return \"\" if 'numeric' is NULL;",
        "- Return a representation of the numeric argument as a value in string.",
        "Note: 'formatStr' defaults to 'yyyy-MM-dd HH:mm:ss'. numeric is an internal timestamp value representing " +
                "seconds since ‘1970-01-01 00:00:00’ UTC, such as produced by the UNIX_TIMESTAMP() function."
}, examples = {"from_unix_time(44) = \"1970-01-01 08:00:44\""})
public class FromUnixTimeFunction implements ValueParser {

    private ValueParser numericParser;
    private ValueParser stringParser;
    private static final Map<String, DateTimeFormatter> OUTPUT_FORMATTERS = new ConcurrentHashMap<>();
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public FromUnixTimeFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        numericParser = OperatorTools.buildParser(expressions.get(0));
        // Determine the number of arguments and build parser
        if (expressions.size() == 2) {
            stringParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numericObj = numericParser.parse(sourceData, rowIndex, context);
        BigDecimal unixTimestamp = OperatorTools.parseBigDecimal(numericObj);
        String formatPattern =
                stringParser != null ? OperatorTools.parseString(stringParser.parse(sourceData, rowIndex, context))
                        : DEFAULT_FORMAT;

        // Convert UNIX timestamp to UTC LocalDateTime
        LocalDateTime utcDateTime =
                LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp.longValue()), ZoneOffset.UTC);
        // Convert UTC LocalDateTime to system default zone LocalDateTime
        LocalDateTime localDateTime =
                utcDateTime.atZone(ZoneOffset.UTC).withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
        return localDateTime.format(getDateTimeFormatter(formatPattern));
    }

    private DateTimeFormatter getDateTimeFormatter(String pattern) {
        DateTimeFormatter formatter = OUTPUT_FORMATTERS.get(pattern);
        if (formatter == null) {
            formatter = DateTimeFormatter.ofPattern(pattern);
            OUTPUT_FORMATTERS.put(pattern, formatter);
        }
        return formatter;
    }
}
