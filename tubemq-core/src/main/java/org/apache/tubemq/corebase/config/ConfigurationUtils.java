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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.tubemq.corebase.utils.TStringUtils;
import org.ini4j.Ini;
import org.ini4j.Profile;

/**
 * Utils, contained methods, is used to check data in configuration.
 */
public enum ConfigurationUtils {

    ;  // Use the features of Enum in java to implement Utils class which holds the static methods.

    public static String checkNotEmpty(String string) {
        if (StringUtils.isEmpty(string)) {
            throw new ConfigurationException("checked empty string.");
        }
        return string;
    }

    public static void checkArgument(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean condition, Object errorMessage) {
        if (!condition) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public static void checkState(boolean condition, String errorMessage) {
        if (!condition) {
            throw new IllegalStateException(errorMessage);
        }
    }

    public static <C> C checkNotNull(C c) {
        checkNotNull(c, "checked null object.");
        return c;
    }

    public static <C> C checkNotNull(C c, String errMsg) {
        if (c == null) {
            throw new ConfigurationException(errMsg);
        }
        return c;
    }

    public static  <E> List<E> buildListIfNull(List<E> list) {
        if (list == null) {
            list = new ArrayList<>();
        }
        return list;
    }

    public static <E> boolean isEmptyArray(E[] array) {
        return array == null || array.length == 0;
    }

    public static <E> boolean isEmptyList(List<E> list) {
        return list == null || list.size() == 0;
    }

    public static List<String> addIfNotExistsWithEmptyCheck(List<String> list, String toAdd, boolean checkEmpty) {
        checkNotNull(list);
        if (!checkEmpty || !StringUtils.isBlank(toAdd)) {
            if (!list.contains(toAdd)) {
                list.add(toAdd);
            }
        }
        return list;
    }

    public static Configuration loadConfiguration(String iniFilePath, String sectionName) {
        checkArgument(TStringUtils.isNotBlank(iniFilePath),
                "iniFilePath must not be empty.");
        checkArgument(TStringUtils.isNotBlank(sectionName),
                "sectionName of 'Ini' file must not be empty.");
        Configuration configuration;
        try {
            final File file = new File(iniFilePath);
            checkState(file.exists() && file.isFile(),
                    String.format("%s must be exists and must be a file.", file.getAbsolutePath()));
            final Ini iniConf = new Ini(file);
            configuration = loadConfiguration(iniConf, sectionName);
        } catch (final IOException e) {
            throw new ConfigurationException("Parse configuration failed, path=" + iniFilePath, e);
        }
        return configuration;
    }

    public static void updateTlsConfiguration(Configuration tlsConfiguration, Configuration newConfiguration) {
        tlsConfiguration.set(TlsConfItems.TLS_ENABLE, newConfiguration.get(TlsConfItems.TLS_ENABLE));
        tlsConfiguration.set(TlsConfItems.TLS_PORT, newConfiguration.get(TlsConfItems.TLS_PORT));
        tlsConfiguration.set(TlsConfItems.TLS_KEY_STORE_PATH,
                newConfiguration.get(TlsConfItems.TLS_KEY_STORE_PATH));
        tlsConfiguration.set(TlsConfItems.TLS_KEY_STORE_PASSWORD,
                newConfiguration.get(TlsConfItems.TLS_KEY_STORE_PASSWORD));
        tlsConfiguration.set(TlsConfItems.TLS_TWO_WAY_AUTH_ENABLE,
                newConfiguration.get(TlsConfItems.TLS_TWO_WAY_AUTH_ENABLE));
        tlsConfiguration.set(TlsConfItems.TLS_TRUST_STORE_PATH,
                newConfiguration.get(TlsConfItems.TLS_TRUST_STORE_PATH));
        tlsConfiguration.set(TlsConfItems.TLS_TRUST_STORE_PASSWORD,
                newConfiguration.get(TlsConfItems.TLS_TRUST_STORE_PASSWORD));
    }

    public static Configuration loadConfiguration(Ini iniConf, String sectionName) {
        Configuration configuration = new Configuration();
        Profile.Section sectioned = iniConf.get(sectionName);
        if (null != sectioned) {
            for (String key : sectioned.keySet()) {
                configuration.setString(key, sectioned.get(key));
            }
        }
        return configuration;
    }
}
