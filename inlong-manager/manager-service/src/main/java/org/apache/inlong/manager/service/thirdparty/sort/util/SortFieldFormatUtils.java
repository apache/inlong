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

package org.apache.inlong.manager.service.thirdparty.sort.util;

import org.apache.inlong.sort.formats.common.ArrayFormatInfo;
import org.apache.inlong.sort.formats.common.BooleanFormatInfo;
import org.apache.inlong.sort.formats.common.ByteFormatInfo;
import org.apache.inlong.sort.formats.common.ByteTypeInfo;
import org.apache.inlong.sort.formats.common.DateFormatInfo;
import org.apache.inlong.sort.formats.common.DecimalFormatInfo;
import org.apache.inlong.sort.formats.common.DoubleFormatInfo;
import org.apache.inlong.sort.formats.common.FloatFormatInfo;
import org.apache.inlong.sort.formats.common.FormatInfo;
import org.apache.inlong.sort.formats.common.IntFormatInfo;
import org.apache.inlong.sort.formats.common.LongFormatInfo;
import org.apache.inlong.sort.formats.common.ShortFormatInfo;
import org.apache.inlong.sort.formats.common.StringFormatInfo;
import org.apache.inlong.sort.formats.common.TimeFormatInfo;
import org.apache.inlong.sort.formats.common.TimestampFormatInfo;

/**
 * Sort field formatting tool
 */
public class SortFieldFormatUtils {

    /**
     * Get the FieldFormat of Sort according to type string
     *
     * @param type type string
     * @return Sort field format instance
     */
    public static FormatInfo convertFieldFormat(String type) {
        FormatInfo formatInfo;
        switch (type) {
            case "boolean":
                formatInfo = new BooleanFormatInfo();
                break;
            case "tinyint":
            case "byte":
                formatInfo = new ByteFormatInfo();
                break;
            case "smallint":
            case "short":
                formatInfo = new ShortFormatInfo();
                break;
            case "int":
                formatInfo = new IntFormatInfo();
                break;
            case "bigint":
            case "long":
                formatInfo = new LongFormatInfo();
                break;
            case "float":
                formatInfo = new FloatFormatInfo();
                break;
            case "double":
                formatInfo = new DoubleFormatInfo();
                break;
            case "decimal":
                formatInfo = new DecimalFormatInfo();
                break;
            case "date":
                formatInfo = new DateFormatInfo();
                break;
            case "time":
                formatInfo = new TimeFormatInfo();
                break;
            case "timestamp":
                formatInfo = new TimestampFormatInfo();
                break;
            case "binary":
            case "fixed":
                formatInfo = new ArrayFormatInfo(ByteTypeInfo::new);
                break;
            default: // default is string
                formatInfo = new StringFormatInfo();
        }

        return formatInfo;
    }

}
