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

import net.sf.jsqlparser.expression.Function;

/**
 * SpaceFunction  ->  SPACE(N)
 * description:
 * - Return NULL if 'N' is NULL
 * - Return "" if 'N' is less than or equal to 0
 * - Return a string consisting of 'N' space characters
 */
@TransformFunction(names = {"space"}, parameter = "(Integer N)", descriptions = {
        "- Return \"\" if 'N' is NULL or less than or equal to 0;",
        "- Return a string consisting of 'N' space characters."
}, examples = {
        "space(5) = \"     \""
})
public class SpaceFunction implements ValueParser {

    private final ValueParser cntParser;

    public SpaceFunction(Function expr) {
        cntParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object cntObj = cntParser.parse(sourceData, rowIndex, context);
        if (cntObj == null) {
            return null;
        }
        int cnt = OperatorTools.parseBigDecimal(cntObj).intValue();
        if (cnt <= 0) {
            return "";
        }
        StringBuilder builder = new StringBuilder(cnt);
        for (int i = 0; i < cnt; i++) {
            builder.append(" ");
        }
        return builder.toString();
    }
}
