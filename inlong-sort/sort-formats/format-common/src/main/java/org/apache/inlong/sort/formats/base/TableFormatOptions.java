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

package org.apache.inlong.sort.formats.base;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Options for table formats.
 */
public class TableFormatOptions {

    public static final ConfigOption<Boolean> IGNORE_ERRORS =
            ConfigOptions.key(TableFormatConstants.FORMAT_IGNORE_ERRORS)
                    .booleanType()
                    .defaultValue(TableFormatConstants.DEFAULT_IGNORE_ERRORS);

    public static final ConfigOption<String> ROW_FORMAT_INFO =
            ConfigOptions.key("row.format.info")
                    .stringType()
                    .noDefaultValue();
}
