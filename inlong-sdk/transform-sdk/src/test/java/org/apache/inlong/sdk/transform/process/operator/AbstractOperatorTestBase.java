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

package org.apache.inlong.sdk.transform.process.operator;

import org.apache.inlong.sdk.transform.pojo.CsvSourceInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.pojo.KvSinkInfo;
import org.apache.inlong.sdk.transform.process.converter.DoubleConverter;
import org.apache.inlong.sdk.transform.process.converter.LongConverter;
import org.apache.inlong.sdk.transform.process.converter.TypeConverter;

import java.util.ArrayList;
import java.util.List;
/**
 * AbstractOperatorTestBase
 * description: define static parameters for Operator tests
 */
public abstract class AbstractOperatorTestBase {

    protected static final List<FieldInfo> srcFields = new ArrayList<>();
    protected static final List<FieldInfo> dstFields = new ArrayList<>();
    protected static final CsvSourceInfo csvSource;
    protected static final KvSinkInfo kvSink;

    static {
        srcFields.add(new FieldInfo("numeric1", new DoubleConverter()));
        srcFields.add(new FieldInfo("string2", TypeConverter.DefaultTypeConverter()));
        srcFields.add(new FieldInfo("numeric3", new DoubleConverter()));
        srcFields.add(new FieldInfo("numeric4", new LongConverter()));

        FieldInfo field = new FieldInfo();
        field.setName("result");
        dstFields.add(field);
        csvSource = new CsvSourceInfo("UTF-8", '|', '\\', srcFields);
        kvSink = new KvSinkInfo("UTF-8", dstFields);
    }
}
