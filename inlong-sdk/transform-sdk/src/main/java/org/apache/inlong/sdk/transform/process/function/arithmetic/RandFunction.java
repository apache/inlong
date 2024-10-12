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
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;

/**
 * RandFunction  ->  Rand([seed])
 * description:
 * - Return a pseudo-random double precision value in the range [0.0, 1.0) if seed is NULL
 * - Return a pseudo-random double precision value in the range [0.0, 1.0) with an initial seed of Integer
 */
@TransformFunction(type = FunctionConstant.ARITHMETIC_TYPE, names = {
        "rand"}, parameter = "(Integer seed)", descriptions = {
                "- Return a pseudo-random double precision value in the range [0.0, 1.0) if seed is NULL;",
                "- Return a pseudo-random double precision value in the range [0.0, 1.0) with an initial 'seed' of Integer."}, examples = {
                        "rand(1)", "rand()"})
public class RandFunction implements ValueParser {

    private ValueParser seedParser;

    private Random random;

    public RandFunction(Function expr) {
        random = new Random();
        if (expr.getParameters() != null) {
            List<Expression> expressions = expr.getParameters().getExpressions();
            if (expressions != null && expressions.size() == 1) {
                seedParser = OperatorTools.buildParser(expressions.get(0));
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (seedParser != null) {
            Object seedObj = seedParser.parse(sourceData, rowIndex, context);
            BigDecimal seedValue = OperatorTools.parseBigDecimal(seedObj);
            random.setSeed(seedValue.intValue());
        }
        return random.nextDouble();
    }
}
