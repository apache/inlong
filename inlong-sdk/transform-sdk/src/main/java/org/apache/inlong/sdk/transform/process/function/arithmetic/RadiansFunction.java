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

/**
 * RadiansFunction  ->  RADIANS(x)
 * description:
 * - Return NULL if x is NULL
 * - Return radians of x, Convert degrees to radians
 */
@TransformFunction(names = {"radians"}, parameter = "(Numeric x)", descriptions = {
        "- Return \"\" if 'x' is NULL;",
        "- Return radians of 'x', Convert degrees to radians."
}, examples = {
        "radians(18.97) = 0.33108895910332425"
})
public class RadiansFunction implements ValueParser {

    private ValueParser degreeParser;

    public RadiansFunction(Function expr) {
        degreeParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object degreeObj = degreeParser.parse(sourceData, rowIndex, context);
        if (degreeObj == null) {
            return null;
        }
        return Math.toRadians(OperatorTools.parseBigDecimal(degreeObj).doubleValue());
    }
}
