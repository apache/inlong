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

package org.apache.inlong.sdk.transform.process.parser;

import org.apache.inlong.sdk.transform.decode.SourceData;
import org.apache.inlong.sdk.transform.process.Context;

import net.sf.jsqlparser.expression.NullValue;

/**
 * NullValueParser
 * <p>
 * Parses the SQL {@code NULL} literal (represented by {@link NullValue}) into a
 * constant Java {@code null}. This avoids {@code ParserTools} falling back to
 * the {@code Column} cast, which previously caused a
 * {@link ClassCastException} for expressions such as {@code ifnull(null, 3)}.
 */
@TransformParser(values = NullValue.class)
public class NullValueParser implements ValueParser {

    public NullValueParser(NullValue expr) {
        // The NULL literal carries no value; nothing to store.
    }

    @Override
    public Object parse(SourceData sourceData, int rowIndex, Context context) {
        return null;
    }
}
