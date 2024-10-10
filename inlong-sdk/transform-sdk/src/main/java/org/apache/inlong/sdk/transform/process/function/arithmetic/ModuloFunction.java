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
 * ModuloFunction
 * description: MOD(NUMERIC1, NUMERIC2) : Return the remainder of numeric1 divided by numeric2.
 */
@TransformFunction(names = {"mod"})
public class ModuloFunction implements ValueParser {

    private ValueParser dividendParser;
    private ValueParser divisorParser;

    public ModuloFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        dividendParser = OperatorTools.buildParser(expressions.get(0));
        divisorParser = OperatorTools.buildParser(expressions.get(1));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object dividendObj = dividendParser.parse(sourceData, rowIndex, context);
        Object divisorObj = divisorParser.parse(sourceData, rowIndex, context);
        BigDecimal dividend = OperatorTools.parseBigDecimal(dividendObj);
        BigDecimal divisor = OperatorTools.parseBigDecimal(divisorObj);
        return dividend.remainder(divisor);
    }
}
