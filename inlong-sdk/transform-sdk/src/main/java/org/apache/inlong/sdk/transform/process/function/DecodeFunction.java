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
@TransformFunction(names = {"decode"})
public class DecodeFunction implements ValueParser {

    private ValueParser binaryParser;

    private ValueParser characterSetParser;

    public DecodeFunction(Function expr) {
        List<Expression> expressions = expr.getParameters().getExpressions();
        if (expressions != null && expressions.size() == 2) {
            binaryParser = OperatorTools.buildParser(expressions.get(0));
            characterSetParser = OperatorTools.buildParser(expressions.get(1));
        }
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object binaryObj = binaryParser.parse(sourceData, rowIndex, context);
        Object characterObj = characterSetParser.parse(sourceData, rowIndex, context);
        if (binaryObj == null || characterObj == null) {
            return null;
        }
        String binaryString = OperatorTools.parseString(binaryObj);
        String characterSetValue = OperatorTools.parseString(characterObj).toUpperCase();
        return decode(binaryString, characterSetValue);
    }

    private String decode(String binaryString, String charsetName) {
        if (binaryString == null || binaryString.isEmpty() || charsetName == null || charsetName.isEmpty()) {
            return "";
        }
        String[] byteValues = binaryString.split(" ");
        byte[] byteArray = new byte[byteValues.length];
        for (int i = 0; i < byteValues.length; i++) {
            byteArray[i] = (byte) Integer.parseInt(byteValues[i]);
        }
        if (Charset.isSupported(charsetName)) {
            Charset charset = Charset.forName(charsetName);
            return new String(byteArray, charset);
        }
        return "";
    }
}
