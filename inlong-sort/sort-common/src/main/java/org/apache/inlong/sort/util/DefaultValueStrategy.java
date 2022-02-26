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

package org.apache.inlong.sort.util;

import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.configuration.Constants;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.ShortFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class DefaultValueStrategy {

    private final Map<String, Object> typeDefaultValues = new HashMap<>();

    public DefaultValueStrategy(Configuration config) {
        // if nullable is true, we don't have to set the value specifically
        // it would get null if there is no key in typeDefaultValues
        if (!config.getBoolean(Constants.SINK_FIELD_TYPE_STRING_NULLABLE)) {
            typeDefaultValues.put(StringFormatInfo.class.getSimpleName(), "");
        }
        if (!config.getBoolean(Constants.SINK_FIELD_TYPE_INT_NULLABLE)) {
            typeDefaultValues.put(IntFormatInfo.class.getSimpleName(), 0);
        }
        if (!config.getBoolean(Constants.SINK_FIELD_TYPE_SHORT_NULLABLE)) {
            typeDefaultValues.put(ShortFormatInfo.class.getSimpleName(), 0);
        }
        if (!config.getBoolean(Constants.SINK_FIELD_TYPE_LONG_NULLABLE)) {
            typeDefaultValues.put(LongFormatInfo.class.getSimpleName(), 0L);
        }
        // TODO, support all types
    }

    public Object getDefaultValue(FormatInfo formatInfo) {
        return typeDefaultValues.get(getTypeKey(formatInfo));
    }

    private static String getTypeKey(FormatInfo formatInfo) {
        return formatInfo.getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultValueStrategy other = (DefaultValueStrategy) o;
        return Objects.equals(typeDefaultValues, other.typeDefaultValues);
    }
}
