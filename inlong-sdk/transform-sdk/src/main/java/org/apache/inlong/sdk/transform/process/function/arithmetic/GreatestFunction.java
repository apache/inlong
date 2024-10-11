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

import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * GreatestFunction  ->  GREATEST(value1[, value2, ...])
 * description:
 * - Return NULL if any parameter is null
 * - Return the greatest value of the list of arguments.
 */
@Slf4j
@TransformFunction(names = {"greatest"}, parameter = "(Numeric value1 [,Numeric value2,...])", descriptions = {
        "- Return \"\" if any parameter is NULL;",
        "- Return the greatest value of the list of arguments."
}, examples = {
        "greatest(3.141592653589793, 3, greatest(4, 1)) = 4"
})
public class GreatestFunction implements ValueParser {

    private List<ValueParser> parserList;

    public GreatestFunction(Function expr) {
        if (expr.getParameters() == null) {
            this.parserList = new ArrayList<>();
        } else {
            List<Expression> params = expr.getParameters().getExpressions();
            parserList = new ArrayList<>(params.size());
            for (Expression param : params) {
                ValueParser node = OperatorTools.buildParser(param);
                parserList.add(node);
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        BigDecimal maxValue = null;
        for (ValueParser valueParser : parserList) {
            Object valueObj = valueParser.parse(sourceData, rowIndex, context);
            if (valueObj == null) {
                return null;
            }

            BigDecimal value = OperatorTools.parseBigDecimal(valueObj);
            if (maxValue == null || value.compareTo(maxValue) > 0) {
                maxValue = value;
            }
        }
        return maxValue;
    }

}
