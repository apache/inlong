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

package org.apache.inlong.sort.formats.inlongmsgbinlog;

import org.apache.inlong.sort.formats.base.FormatDescriptorValidator;

import org.apache.flink.table.descriptors.DescriptorProperties;

import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_IGNORE_ERRORS;
import static org.apache.inlong.sort.formats.base.TableFormatConstants.FORMAT_SCHEMA;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_ATTRIBUTES_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsg.row.InLongMsgUtils.FORMAT_TIME_FIELD_NAME;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.FORMAT_INCLUDE_UPDATE_BEFORE;
import static org.apache.inlong.sort.formats.inlongmsgbinlog.InLongMsgBinlogUtils.FORMAT_METADATA_FIELD_NAME;

public class InLongMsgBinlogValidator extends FormatDescriptorValidator {

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);

        properties.validateString(FORMAT_SCHEMA, false);
        properties.validateString(FORMAT_TIME_FIELD_NAME, true, 1);
        properties.validateString(FORMAT_ATTRIBUTES_FIELD_NAME, true, 1);
        properties.validateString(FORMAT_METADATA_FIELD_NAME, true, 1);
        properties.validateBoolean(FORMAT_IGNORE_ERRORS, true);
        properties.validateBoolean(FORMAT_INCLUDE_UPDATE_BEFORE, true);
    }

}
