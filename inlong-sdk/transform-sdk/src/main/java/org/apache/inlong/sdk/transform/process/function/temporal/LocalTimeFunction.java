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

import net.sf.jsqlparser.expression.Function;

import java.time.LocalTime;
import java.time.ZoneId;

/**
 * LocalTimeFunction  ->  localTime([timeZoneStr])
 * description:
 * - Return the current time in the specified time zone.
 * Note: timeZoneStr is the system time zone
 */
@TransformFunction(type = FunctionConstant.TEMPORAL_TYPE, names = {"localtime",
        "current_time"}, parameter = "([String timeZoneStr])", descriptions = {
                "- Return the current time in the specified time zone."}, examples = {"localTime() = currentTime",
                        "currentTime(\"UTC\") = currentTime"})
public class LocalTimeFunction implements ValueParser {

    private ValueParser stringParser;

    public LocalTimeFunction(Function expr) {
        if (expr.getParameters() != null) {
            stringParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (stringParser != null) {
            String zoneString = OperatorTools.parseString(stringParser.parse(sourceData, rowIndex, context));
            return LocalTime.now(ZoneId.of(zoneString)).withNano(0);
        } else {
            return LocalTime.now(ZoneId.systemDefault()).withNano(0);
        }
    }
}
