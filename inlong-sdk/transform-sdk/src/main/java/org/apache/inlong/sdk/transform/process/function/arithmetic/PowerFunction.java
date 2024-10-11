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

import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;

/**
 * PowerFunction  ->  power(numeric1, numeric2)
 * description:
 * - Return NULL if any parameter is NULL
 * - Return 'numeric1'.power('numeric2')
 */
@TransformFunction(names = {"power"}, parameter = "(Numeric numeric1, Numeric numeric2)", descriptions = {
        "- Return \"\" if any parameter is NULL.",
        "- Return 'numeric1'.power('numeric2')."
}, examples = {
        "power(4,0.5) = 2.0"
})
public class PowerFunction implements ValueParser {

    private ValueParser baseParser;
    private ValueParser exponentParser;

    public PowerFunction(Function expr) {
        baseParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
        exponentParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object baseObj = baseParser.parse(sourceData, rowIndex, context);
        Object exponentObj = exponentParser.parse(sourceData, rowIndex, context);
        if (baseObj == null || exponentObj == null) {
            return null;
        }
        BigDecimal baseValue = OperatorTools.parseBigDecimal(baseObj);
        BigDecimal exponentValue = OperatorTools.parseBigDecimal(exponentObj);
        return Math.pow(baseValue.doubleValue(), exponentValue.doubleValue());
    }
}
