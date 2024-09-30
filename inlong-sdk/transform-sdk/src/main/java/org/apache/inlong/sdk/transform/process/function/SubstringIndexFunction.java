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

import java.util.ArrayList;
import java.util.List;

/**
 * SubstringIndexFunction  -> SUBSTRING_INDEX(str,delim,count)
 * description: Returns the substring from string str before count occurrences of the delimiter delim
 * return NULL if any parameter is NULL;
 * return everything to the left of the final delimiter (counting from the left) if count is positive;
 * return everything to the right of the final delimiter (counting from the right) if count is negative.
 */
@TransformFunction(names = {"substring_index"})
public class SubstringIndexFunction implements ValueParser {

    private ValueParser stringParser;
    private ValueParser delimParser;
    private ValueParser countParser;

    public SubstringIndexFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        stringParser = OperatorTools.buildParser(expressions.get(0));
        delimParser = OperatorTools.buildParser(expressions.get(1));
        countParser = OperatorTools.buildParser(expressions.get(2));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object stringObj = stringParser.parse(sourceData, rowIndex, context);
        Object delimObj = delimParser.parse(sourceData, rowIndex, context);
        Object countObj = countParser.parse(sourceData, rowIndex, context);
        if (stringObj == null || delimObj == null || countObj == null) {
            return null;
        }
        String str = OperatorTools.parseString(stringObj);
        String delim = OperatorTools.parseString(delimObj);
        int count = OperatorTools.parseBigDecimal(countObj).intValue();
        if (str.isEmpty() || delim.isEmpty() || count == 0) {
            return "";
        }
        boolean isRight = count < 0;
        count = Math.abs(count);
        ArrayList<Integer> startIndexList = findStart(delim, str);
        int size = startIndexList.size();
        if (size < count) {
            return str;
        }
        if (isRight) {
            int start = startIndexList.get(size - count);
            return str.substring(start + delim.length());
        } else {
            int start = startIndexList.get(count - 1);
            return str.substring(0, start);
        }
    }

    /**
     * Use kmp to find all the starting subscripts of the pattern from str
     *
     * @param patten Pattern string
     * @param str    target string
     * @return starting index list
     */
    public ArrayList<Integer> findStart(String patten, String str) {
        ArrayList<Integer> next = new ArrayList<>();
        ArrayList<Integer> startIndexList = new ArrayList<>();
        next.add(0);
        for (int i = 1, j = 0; i < patten.length(); i++) {
            while (j > 0 && patten.charAt(i) != patten.charAt(j)) {
                j = next.get(j - 1);
            }
            if (patten.charAt(i) == patten.charAt(j)) {
                j++;
            }
            next.add(j);
        }
        for (int i = 0, j = 0; i < str.length(); i++) {
            while (j > 0 && str.charAt(i) != patten.charAt(j)) {
                j = next.get(j - 1);
            }
            if (str.charAt(i) == patten.charAt(j)) {
                j++;
            }
            if (j == patten.length()) {
                startIndexList.add(i - patten.length() + 1);
                j = 0;
            }
        }
        return startIndexList;
    }
}
