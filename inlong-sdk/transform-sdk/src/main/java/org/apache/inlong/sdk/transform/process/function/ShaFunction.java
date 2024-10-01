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

import net.sf.jsqlparser.expression.Function;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * ShaFunction
 * description: sha(string): Compute the SHA-1 160 bit checksum of a string.
 * return NULL if the parameter is NULL
 * return a string of 40 hexadecimal digits.
 */
@TransformFunction(names = {"sha"})
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
