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
import org.apache.inlong.sdk.transform.process.parser.ValueParser;

import net.sf.jsqlparser.expression.Function;
/**
 * PiFunction  ->  pi()
 * description:
 * - Return the mathematical constant PI
 */
@TransformFunction(names = {"pi"}, parameter = "()", descriptions = {
        "- Return the mathematical constant PI."
}, examples = {
        "pi() = 3.141592653589793"
})
public class PiFunction implements ValueParser {

    public PiFunction(Function expr) {
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        return String.valueOf(Math.PI);
    }

}
