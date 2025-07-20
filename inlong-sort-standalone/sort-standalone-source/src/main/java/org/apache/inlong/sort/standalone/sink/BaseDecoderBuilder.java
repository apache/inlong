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

package org.apache.inlong.sort.standalone.sink;

import org.apache.inlong.common.pojo.sort.dataflow.field.FieldConfig;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.BasicFormatInfo;
import org.apache.inlong.common.pojo.sort.dataflow.field.format.FormatInfo;
import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.process.converter.TypeConverter;

public abstract class BaseDecoderBuilder implements IDecoderBuilder {

    public FieldInfo convertToTransformFieldInfo(FieldConfig config) {
        return new FieldInfo(config.getName(), deriveTypeConverter(config.getFormatInfo()));
    }

    public TypeConverter deriveTypeConverter(FormatInfo formatInfo) {

        if (formatInfo instanceof BasicFormatInfo) {
            return value -> ((BasicFormatInfo<?>) formatInfo).deserialize(value);
        }
        return value -> value;
    }
}
