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

package org.apache.inlong.sdk.transform.process.function.arithmetic;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;
import java.util.List;

/**
 * LogFunction  ->  log(numeric1,[numeric2])
 * description:
 * - Return the natural logarithm of 'numeric1' when called with one argument
 * - Return the logarithm of 'numeric2' to the base 'numeric1' when called with two arguments
 */
@TransformFunction(names = {"log"}, parameter = "(Numeric numeric1 [, Numeric numeric2])", descriptions = {
        "- Return the natural logarithm of 'numeric1' when called with one argument;",
        "- Return the logarithm of 'numeric2' to the base 'numeric1' when called with two arguments."
}, examples = {
        "log(1) = 0.0",
        "log(2,8) = 3.0"
})
public class LogFunction implements ValueParser {

    private ValueParser baseParser;
    private final ValueParser numberParser;

    public LogFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        // Determine the number of arguments and build parser
        if (expressions.size() == 1) {
            numberParser = OperatorTools.buildParser(expressions.get(0));
        } else {
            baseParser = OperatorTools.buildParser(expressions.get(0));
            numberParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numberObj = numberParser.parse(sourceData, rowIndex, context);
        BigDecimal numberValue = OperatorTools.parseBigDecimal(numberObj);
        if (baseParser != null) {
            Object baseObj = baseParser.parse(sourceData, rowIndex, context);
            BigDecimal baseValue = OperatorTools.parseBigDecimal(baseObj);
            return Math.log(numberValue.doubleValue()) / Math.log(baseValue.doubleValue());
        } else {
            return Math.log(numberValue.doubleValue());
        }
    }
}
