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

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

/**
 * TimestampExtractFunction
 * description:
 * - hour(timestamp)--returns the hour of a day (an integer between 0 and 23) from SQL timestamp
 * - minute(timestamp)--returns the minute of an hour (an integer between 0 and 59) from SQL timestamp
 * - second(timestamp)--returns the second of a minute (an integer between 0 and 59) from SQL timestamp
 */
public abstract class TimestampExtractFunction implements ValueParser {

    private TimestampExtractFunctionType type;
    private ValueParser timestampParser;

    public enum TimestampExtractFunctionType {
        HOUR, MINUTE, SECOND
    }

    @TransformFunction(type = FunctionConstant.TEMPORAL_TYPE, names = {
            "hour"}, parameter = "(String timestamp)", descriptions = {
                    "- Return \"\" if 'timestamp' is null;",
                    "- Return the hour of a day (an integer between 0 and 23) from SQL 'timestamp'."
            }, examples = {"hour(2024-08-12 12:23:34) = 12"})
    public static class HourExtractFunction extends TimestampExtractFunction {

        public HourExtractFunction(Function expr) {
            super(TimestampExtractFunctionType.HOUR, expr);
        }
    }

    @TransformFunction(type = FunctionConstant.TEMPORAL_TYPE, names = {
            "minute"}, parameter = "(String timestamp)", descriptions = {
                    "- Return \"\" if 'timestamp' is null;",
                    "- Return the minute of an hour (an integer between 0 and 59) from SQL 'timestamp'."
            }, examples = {"minute(2024-08-12 12:23:34) = 23"})
    public static class MinuteExtractFunction extends TimestampExtractFunction {

        public MinuteExtractFunction(Function expr) {
            super(TimestampExtractFunctionType.MINUTE, expr);
        }
    }

    @TransformFunction(type = FunctionConstant.TEMPORAL_TYPE, names = {
            "second"}, parameter = "(String timestamp)", descriptions = {
                    "- Return \"\" if 'timestamp' is null;",
                    "- Return the second of a minute (an integer between 0 and 59) from SQL 'timestamp'."
            }, examples = {"second(2024-08-12 12:23:34) = 34"})
    public static class SecondExtractFunction extends TimestampExtractFunction {

        public SecondExtractFunction(Function expr) {
            super(TimestampExtractFunctionType.SECOND, expr);
        }
    }

    public TimestampExtractFunction(TimestampExtractFunctionType type, Function expr) {
        this.type = type;
        List<Expression> expressions = expr.getParameters().getExpressions();
        timestampParser = OperatorTools.buildParser(expressions.get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object timestampObj = timestampParser.parse(sourceData, rowIndex, context);
        Timestamp timestamp = OperatorTools.parseTimestamp(timestampObj);
        LocalDateTime localDateTime = timestamp.toLocalDateTime();
        switch (type) {
            // hour(between 0 and 23)
            case HOUR:
                return localDateTime.getHour();
            // minute(between 0 and 59)
            case MINUTE:
                return localDateTime.getMinute();
            // second(between 0 and 59)
            case SECOND:
                return localDateTime.getSecond();
            default:
                return null;
        }
    }
}
