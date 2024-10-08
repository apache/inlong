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

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
/**
 * ConvertTzFunction
 * Description: CONVERT_TZ(string1, string2, string3)--Converts a datetime string1 (with default ISO timestamp format
 *              ‘yyyy-MM-dd HH:mm:ss’) from time zone string2 to time zone string3. The format of time zone should be
 *              either an abbreviation such as “PST”, a full name such as “America/Los_Angeles”, or a custom ID such
 *              as “GMT-08:00”.
 * for example: CONVERT_TZ(‘1970-01-01 00:00:00’, ‘UTC’, ‘America/Los_Angeles’) returns ‘1969-12-31 16:00:00’.
 */
@TransformFunction(names = {"convert_tz"})
public class ConvertTzFunction implements ValueParser {

    private ValueParser dateTimeParser;

    private ValueParser fromTimeZoneParser;

    private ValueParser toTimeZoneParser;

    public ConvertTzFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        dateTimeParser = OperatorTools.buildParser(expressions.get(0));
        fromTimeZoneParser = OperatorTools.buildParser(expressions.get(1));
        toTimeZoneParser = OperatorTools.buildParser(expressions.get(2));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object datetimeObj = dateTimeParser.parse(sourceData, rowIndex, context);
        Object fromTimeZoneObj = fromTimeZoneParser.parse(sourceData, rowIndex, context);
        Object toTimeZoneObj = toTimeZoneParser.parse(sourceData, rowIndex, context);
        if (datetimeObj == null || fromTimeZoneObj == null || toTimeZoneObj == null) {
            return null;
        }
        String dateTime = OperatorTools.parseString(datetimeObj);
        String fromTimeZone = OperatorTools.parseString(fromTimeZoneObj);
        String toTimeZone = OperatorTools.parseString(toTimeZoneObj);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, formatter);

        ZoneId fromZoneId = ZoneId.of(fromTimeZone);
        ZoneId toZoneId = ZoneId.of(toTimeZone);

        ZonedDateTime fromZonedDateTime = ZonedDateTime.of(localDateTime, fromZoneId);
        ZonedDateTime toZonedDateTime = fromZonedDateTime.withZoneSameInstant(toZoneId);

        return toZonedDateTime.format(formatter);
    }
}
