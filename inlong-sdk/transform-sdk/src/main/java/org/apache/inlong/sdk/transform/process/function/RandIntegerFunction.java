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
import java.util.Random;

/**
 * RandIntegerFunction
 * description: RAND_INTEGER(INT1)--Returns a pseudorandom integer value in the range [0, INT)
 *              RAND_INTEGER(INT1, INT2)--Returns a pseudorandom integer value in the range [0, INT1) with an initial seed INT2.
 *              Two RAND_INTEGER functions will return idential sequences of numbers if they have the same initial seed and bound.
 */
@TransformFunction(names = {"rand_integer"})
public class RandIntegerFunction implements ValueParser {

    private ValueParser firstIntParser;

    private ValueParser secondIntParser;

    private Random random;

    public RandIntegerFunction(Function expr) {
        random = new Random();
        List<Expression> expressions = expr.getParameters().getExpressions();
        if (expressions != null) {
            firstIntParser = OperatorTools.buildParser(expressions.get(0));
            if (expressions.size() >= 2) {
                secondIntParser = OperatorTools.buildParser(expressions.get(1));
            }
        }
    }
    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object firstIntObj = firstIntParser.parse(sourceData, rowIndex, context);
        int firstInt = OperatorTools.parseBigDecimal(firstIntObj).intValue();
        if (secondIntParser != null) {
            Object secondIntObj = secondIntParser.parse(sourceData, rowIndex, context);
            int secondInt = OperatorTools.parseBigDecimal(secondIntObj).intValue();
            return randInteger(firstInt, secondInt);
        }
        return randInteger(firstInt);
    }

    private int randInteger(int range) {
        return random.nextInt(range);
    }

    private int randInteger(int range, int seed) {
        random.setSeed(seed);
        return random.nextInt(range);
    }
}
