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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RegexpExtractAllFunction
 * description: REGEXP_EXTRACT_ALL(str, regexp[, extractIndex])--Returns an ARRAY representation of all the matched substrings.
 *              NULL if any of the arguments are NULL or invalid.Extracts all the substrings in str that match the regexp
 *              expression and correspond to the regexp group extractIndex. regexp may contain multiple groups. extractIndex
 *              indicates which regexp group to extract and starts from 1, also the default value if not specified.
 *              0 means matching the entire regular expression.
 * for example: REGEXP_EXTRACT_ALL("abc123def456ghi789", "(\\d+)", 0)--return [123, 456, 789]
 *              REGEXP_EXTRACT_ALL("Name: John, Age: 25, Location: NY", "Name: (\\w+), Age: (\\d+), Location: (\\w+)", 1)--return [John]
 *              REGEXP_EXTRACT_ALL("Name: John, Age: 25, Location: NY", "Name: (\\w+), Age: (\\d+), Location: (\\w+)", 0)--return [Name: John, Age: 25, Location: NY]
 */
@TransformFunction(names = {"regexp_extract_all"})
public class RegexpExtractAllFunction implements ValueParser {

    private ValueParser inputStringParser;

    private ValueParser patternStringParser;

    private ValueParser indexIntegerParser;

    public RegexpExtractAllFunction(Function expr) {
        if (expr.getParameters() != null) {
            List<Expression> expressions = expr.getParameters().getExpressions();
            if (expressions != null && expressions.size() >= 2) {
                inputStringParser = OperatorTools.buildParser(expressions.get(0));
                patternStringParser = OperatorTools.buildParser(expressions.get(1));
                if (expressions.size() >= 3) {
                    indexIntegerParser = OperatorTools.buildParser(expressions.get(2));
                }
            }
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        if (inputStringParser == null || patternStringParser == null) {
            return null;
        }
        String inputString = OperatorTools.parseString(inputStringParser.parse(sourceData, rowIndex, context));
        String patternString = OperatorTools.parseString(patternStringParser.parse(sourceData, rowIndex, context));
        int index = 0;
        if (indexIntegerParser != null) {
            index = OperatorTools.parseBigDecimal(indexIntegerParser.parse(sourceData, rowIndex, context)).intValue();
        }
        if (index < 0) {
            return null;
        }
        List<String> resultList = new ArrayList<>();

        Pattern pattern = Pattern.compile(patternString);
        Matcher matcher = pattern.matcher(inputString);
        while (matcher.find()) {
            if (index <= matcher.groupCount()) {
                resultList.add(matcher.group(index));
            } else {
                return null;
            }
        }

        return resultList.isEmpty() ? null : resultList;
    }
}
