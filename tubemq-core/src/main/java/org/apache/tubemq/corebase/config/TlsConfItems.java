/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.corebase.config;


import org.apache.tubemq.corebase.config.converter.builtin.BooleanConverter;
import org.apache.tubemq.corebase.config.converter.builtin.IntegerConverter;
import org.apache.tubemq.corebase.config.converter.builtin.StringConverter;

/**
 * The set of configuration options for TLS parameters.
 */
public class TlsConfItems {

    /**
     * Whether to enable TLS function, optional configuration, default is false.
     */
    public static final ConfigItem<Boolean> TLS_ENABLE = ConfigItem.<Boolean>builder()
            .key("tlsEnable")
            .defaultValue(false)
            .valueConverter(BooleanConverter.getInstance())
            .description("Whether to enable TLS function, optional configuration, default is false.")
            .build();

    /**
     * TLS port number, optional configuration.
     */
    public static final ConfigItem<Integer> TLS_PORT = ConfigItem.<Integer>builder()
            .key("tlsPort")
            .defaultValue(8716)
            .valueConverter(IntegerConverter.getInstance())
            .description("Master TLS port number, optional configuration, default is 8716;\n "
                    + "Broker TLS port number, optional configuration, default is 8124.")
            .build();

    /**
     * The absolute storage path of the TLS keyStore file + the name of the keyStore file.
     * This field is required and cannot be empty when the TLS function {@link #TLS_ENABLE} is enabled.
     */
    public static final ConfigItem<String> TLS_KEY_STORE_PATH = ConfigItem.<String>builder()
            .key("tlsKeyStorePath")
            .defaultValue("")
            .valueConverter(StringConverter.getInstance())
            .description("The absolute storage path of the TLS keyStore " +
                    "file + the name of the keyStore file. This field is required " +
                    "and cannot be empty when the TLS function is enabled.")
            .build();

    public static final ConfigItem<String> TLS_KEY_STORE_PASSWORD = ConfigItem.<String>builder()
            .key("tlsKeyStorePassword")
            .valueConverter(StringConverter.getInstance())
            .defaultValue("")
            .description("The absolute storage path of the TLS keyStorePassword " +
                    "file + the name of the keyStorePassword file. This field is required " +
                    "and cannot be empty when the TLS function is enabled.")
            .build();

    public static final ConfigItem<Boolean> TLS_TWO_WAY_AUTH_ENABLE = ConfigItem.<Boolean>builder()
            .key("tlsTwoWayAuthEnable")
            .defaultValue(false)
            .valueConverter(BooleanConverter.getInstance())
            .description("Whether to enable TLS mutual authentication, optional configuration, the default is false.")
            .build();

    public static final ConfigItem<String> TLS_TRUST_STORE_PATH = ConfigItem.<String>builder()
            .key("tlsTrustStorePath")
            .defaultValue("")
            .valueConverter(StringConverter.getInstance())
            .description("The absolute storage path of the TLS TrustStore file + the" +
                    " TrustStore file name. This field is required and cannot be empty when" +
                    " the TLS function is enabled and mutual authentication is enabled.")
            .build();

    public static final ConfigItem<String> TLS_TRUST_STORE_PASSWORD = ConfigItem.<String>builder()
            .key("tlsTrustStorePassword")
            .defaultValue("")
            .valueConverter(StringConverter.getInstance())
            .description("The absolute storage path of the TLS TrustStorePassword " +
                    "file + the TrustStorePassword file name. This field is required and " +
                    "cannot be empty when the TLS function is enabled and mutual " +
                    "authentication is enabled.")
            .build();
}
