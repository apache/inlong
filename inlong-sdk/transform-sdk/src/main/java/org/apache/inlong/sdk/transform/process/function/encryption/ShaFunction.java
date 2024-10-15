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

package org.apache.inlong.sdk.transform.process.function.encryption;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;
import org.apache.inlong.sdk.transform.process.function.FunctionConstant;
import org.apache.inlong.sdk.transform.process.function.TransformFunction;
import org.apache.inlong.sdk.transform.process.operator.OperatorTools;
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Function;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * ShaFunction  ->  sha(str)
 * description:
 * - Return NULL if 'str' is NULL
 * - Return a string of 40 hexadecimal digits (the SHA-1 160 bit)
 */
@TransformFunction(type = FunctionConstant.ENCRYPTION_TYPE, names = {
        "sha"}, parameter = "(String str)", descriptions = {
                "- Return \"\" if 'str' is NULL;",
                "- Return a string of 40 hexadecimal digits (the SHA-1 160 bit)."
        }, examples = {"sha(\"5\") = \"ac3478d69a3c81fa62e60f5c3696165a4e5e6ac4\""})
public class ShaFunction implements ValueParser {

    private final ValueParser msgParser;

    public ShaFunction(Function expr) {
        msgParser = OperatorTools.buildParser(expr.getParameters().getExpressions().get(0));
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        Object msgObj = msgParser.parse(sourceData, rowIndex, context);
        if (msgObj == null) {
            return null;
        }
        return DigestUtils.sha1Hex(OperatorTools.parseBytes(msgObj));
    }
}
