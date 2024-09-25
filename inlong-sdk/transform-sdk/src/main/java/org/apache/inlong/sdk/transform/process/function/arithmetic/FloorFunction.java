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
 * FloorFunction
 * description: floor(numeric)--rounds numeric down, and returns the largest number that is less than or equal to numeric
 */
@TransformFunction(names = {"floor"})
public class FloorFunction implements ValueParser {

    private ValueParser numberParser;

    /**
     * Constructor
     * @param expr
     */
    public FloorFunction(Function expr) {
        numberParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    /**
     * parse
     * @param sourceData
     * @param rowIndex
     * @return
     */
    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object numberObj = numberParser.parse(sourceData, rowIndex, context);
        BigDecimal numberValue = OperatorTools.parseBigDecimal(numberObj);
        return Math.floor(numberValue.doubleValue());
    }
}