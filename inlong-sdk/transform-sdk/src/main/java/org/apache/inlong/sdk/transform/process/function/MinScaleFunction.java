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

import java.util.List;

/**
 * MinScaleFunction  ->  min_scale(numeric)
 * description:
 * return NULL if numeric is NULL
 * return minimum scale (number of fractional decimal digits) needed to represent the supplied value precisely
 */
@TransformFunction(names = {"min_scale"})
public class MinScaleFunction implements ValueParser {

    private final ValueParser numParser;

    public MinScaleFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        numParser = OperatorTools.buildParser(expressions.get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numObj = numParser.parse(sourceData, rowIndex, context);
        if (numObj == null) {
            return null;
        }
        return OperatorTools.parseBigDecimal(numObj).stripTrailingZeros().scale();
    }
}
