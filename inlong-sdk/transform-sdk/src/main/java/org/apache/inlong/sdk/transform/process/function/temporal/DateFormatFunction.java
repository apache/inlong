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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DateFormatFunction  ->  date_format(timestamp,format)
 * Description:
 * - Return NULL if any parameter is NULL
 * - Return a string value that converts a timestamp (in seconds) to a date format string in the specified format
 * Note: The format string is compatible with Java’s SimpleDateFormat
 */
@TransformFunction(type = FunctionConstant.TEMPORAL_TYPE, names = {
        "date_format"}, parameter = "(String timestampStr, String formatStr)", descriptions = {
                "- Return \"\" if any parameter is NULL;",
                "- Return a string value that converts a timestamp (in seconds) to a date format string in the specified format.",
                "Note: The format string is compatible with Java’s SimpleDateFormat"}, examples = {
                        "date_format('2024-08-01 22:56:56', 'yyyy/MM/dd HH:mm:ss') = \"2024/08/01 22:56:56\""})
public class DateFormatFunction implements ValueParser {

    private ValueParser timestampParser;
    private ValueParser formatParser;
    private static final Map<String, SimpleDateFormat> SIMPLE_DATE_FORMATS = new ConcurrentHashMap<>();

    public DateFormatFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        timestampParser = OperatorTools.buildParser(expressions.get(0));
        formatParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object timestampObj = timestampParser.parse(sourceData, rowIndex, context);
        Object formatObj = formatParser.parse(sourceData, rowIndex, context);
        if (timestampObj == null || formatObj == null) {
            return null;
        }
        Timestamp timestamp = OperatorTools.parseTimestamp(timestampObj);
        String format = OperatorTools.parseString(formatObj);
        SimpleDateFormat sdf = getSimpleDateFormat(format);
        Date date = new Date(timestamp.getTime());
        return sdf.format(date);
    }

    private SimpleDateFormat getSimpleDateFormat(String pattern) {
        SimpleDateFormat sdf = SIMPLE_DATE_FORMATS.get(pattern);
        if (sdf == null) {
            sdf = new SimpleDateFormat(pattern);
            SIMPLE_DATE_FORMATS.put(pattern, sdf);
        }
        return sdf;
    }
}
