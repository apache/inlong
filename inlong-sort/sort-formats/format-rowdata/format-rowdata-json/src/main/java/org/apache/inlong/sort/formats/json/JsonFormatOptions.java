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

package org.apache.inlong.sort.formats.json;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * The options of Json.
 */
public class JsonFormatOptions {

    public static final ConfigOption<Boolean> AUTO_CLOSE_SOURCE =
            ConfigOptions.key("auto-close-source")
                    .booleanType()
                    .defaultValue(true);

    public static final ConfigOption<Boolean> ALLOW_COMMENTS =
            ConfigOptions.key("allow-comments")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> ALLOW_YAML_COMMENTS =
            ConfigOptions.key("allow-yaml-comments")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> ALLOW_UNQUOTED_FIELD_NAMES =
            ConfigOptions.key("allow-unquoted-field-names")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> ALLOW_SINGLE_QUOTES =
            ConfigOptions.key("allow-single-quotes")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> ALLOW_UNQUOTED_CONTROL_CHARS =
            ConfigOptions.key("allow-unquoted-control-chars")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER =
            ConfigOptions.key("allow-backslash-escaping-any-character")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> ALLOW_NUMERIC_LEADING_ZEROS =
            ConfigOptions.key("allow-numeric-leading-zeros")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> ALLOW_NON_NUMERIC_NUMBERS =
            ConfigOptions.key("allow-numeric-leading-zeros")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> ALLOW_MISSING_VALUES =
            ConfigOptions.key("allow-missing-values")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> ALLOW_TRAILING_COMMA =
            ConfigOptions.key("allow-trailing-comma")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> STRICT_DUPLICATE_DETECTION =
            ConfigOptions.key("strict-duplicate-detection")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> IGNORE_UNDEFINED =
            ConfigOptions.key("ignore-undefined")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> INCLUDE_SOURCE_IN_LOCATION =
            ConfigOptions.key("include-source-in-location")
                    .booleanType()
                    .defaultValue(true);

    private JsonFormatOptions() {
    }
}
