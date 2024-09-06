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

import java.nio.charset.Charset;
import java.util.List;
@TransformFunction(names = {"encode"})
public class EncodeFunction implements ValueParser {

    private ValueParser stringParser;

    private ValueParser characterSetParser;

    public EncodeFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        if (expressions != null && expressions.size() == 2) {
            stringParser = OperatorTools.buildParser(expressions.get(0));
            characterSetParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        String stringValue = OperatorTools.parseString(stringParser.parse(sourceData, rowIndex, context));
        String characterSetValue =
                OperatorTools.parseString(characterSetParser.parse(sourceData, rowIndex, context)).toUpperCase();
        byte[] encodeBytes = encode(stringValue, characterSetValue);
        StringBuilder res = new StringBuilder();
        if (encodeBytes != null) {
            for (byte encodeByte : encodeBytes) {
                res.append((int) encodeByte).append(" ");
            }
        }
        return res.toString().trim();
    }

    private byte[] encode(String stringValue, String characterSetValue) {
        if (stringValue == null || stringValue.isEmpty() || characterSetValue == null || characterSetValue.isEmpty()) {
            return new byte[0];
        }
        if (Charset.isSupported(characterSetValue)) {
            Charset charset = Charset.forName(characterSetValue);
            return stringValue.getBytes(charset);
        }
        return null;
    }
}
