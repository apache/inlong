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

package org.apache.inlong.sdk.transform.process.function.string;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.List;
/**
 * RepeatFunction
 * description: repeat(string, numeric)--Repeat the string numeric times and return a new string
 *              replicate(string, numeric)--Repeat the string numeric times and return a new string
 */
@TransformFunction(names = {"repeat", "replicate"})
public class RepeatFunction implements ValueParser {

    private ValueParser stringParser;

    private ValueParser countParser;

    public RepeatFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        stringParser = OperatorTools.buildParser(expressions.get(0));
        countParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObj = stringParser.parse(sourceData, rowIndex, context);
        Object countObj = countParser.parse(sourceData, rowIndex, context);
        if (stringObj == null || countObj == null) {
            return null;
        }
        if (stringObj.equals("") || countObj.equals("")) {
            return "";
        }
        String str = OperatorTools.parseString(stringObj);
        double count = OperatorTools.parseBigDecimal(countObj).doubleValue();
        return repeat(str, count);
    }
    private String repeat(String str, double count) {
        if (count == 0) {
            return "";
        }
        if (count == 1) {
            return str;
        }
        StringBuilder repeatedStr = new StringBuilder();
        StringBuilder originStr = new StringBuilder(str);
        while (count > 0) {
            if (count % 2 != 0) {
                repeatedStr.append(originStr);
            }
            count = Math.floor(count / 2);
            originStr.append(originStr);
        }
        return repeatedStr.toString();
    }
}
