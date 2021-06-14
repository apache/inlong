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

import java.util.List;

import org.apache.tubemq.corebase.config.converter.Converter;
import org.apache.tubemq.corebase.config.converter.builtin.DefaultConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigItem<V> extends Converter<Object, V> {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConfigItem.class);

    private final String key;
    private final V defaultValue;
    private final Class<?> valueClass;
    /**
     * the deprecatedKeys are ordered by priority.
     */
    private final List<String> deprecatedKeys;
    private final String description;
    private final Converter<Object, V> valueConverter;

    private ConfigItem(String key, V defaultValue, Class<?> valueClass, List<String> deprecatedKeys,
                       String description, Converter<Object, V> valueConverter) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.valueClass = valueClass;
        this.deprecatedKeys = deprecatedKeys;
        this.description = description;
        this.valueConverter = valueConverter;
    }

    public static <T> boolean isEmpty(T[] array) {
        return array == null || array.length == 0;
    }

    public String getKey() {
        return key;
    }

    public V getDefaultValue() {
        return defaultValue;
    }

    public Class<V> getValueClass() {
        return (Class<V>) valueClass;
    }

    public List<String> getDeprecatedKeys() {
        return deprecatedKeys;
    }

    public String getDescription() {
        return description;
    }

    public Converter<Object, V> getValueConvertor() {
        return valueConverter;
    }

    public boolean isDefaulted() {
        return this.defaultValue != null;
    }

    @Override
    public String toString() {
        return "ConfigItem{" +
                "key='" + key + '\'' +
                ", defaultValue=" + defaultValue +
                ", valueClass=" + valueClass +
                ", deprecatedKeys=" + deprecatedKeys +
                ", description='" + description + '\'' +
                ", valueConverter=" + valueConverter +
                '}';
    }

    public static <V> ConfigItemBuilder<V> builder() {
        return new ConfigItemBuilder<V>();
    }

    @Override
    protected V convert(Object o) {
        return this.valueConverter.doConvert(o);
    }

    public static class ConfigItemBuilder<V> {
        private String key;
        private V defaultValue;
        private Class<?> valueClass;
        private List<String> deprecatedKeys;
        private String description = "default empty description.";
        private Converter<Object, V> valueConverter = (Converter<Object, V>) DefaultConverter.getInstance();

        private ConfigItemBuilder() {
        }

        public ConfigItemBuilder<V> key(String key) {
            this.key = ConfigurationUtils.checkNotEmpty(key);
            return ConfigItemBuilder.this;
        }

        public ConfigItemBuilder<V> defaultValue(V defaultValue) {
            this.defaultValue = ConfigurationUtils.checkNotNull(defaultValue);
            this.valueClass = defaultValue.getClass();
            return ConfigItemBuilder.this;
        }

        public ConfigItemBuilder<V> nonDefaultValue(Class<V> valueClass) {
            this.valueClass = ConfigurationUtils.checkNotNull(valueClass);
            return ConfigItemBuilder.this;
        }

        /**
         * declare the value type.
         * use {@link #nonDefaultValue(Class)} instead of this method here.
         */
        public ConfigItemBuilder<V> valueClass(Class<V> valueClass) {
            return nonDefaultValue(valueClass);
        }

        public ConfigItemBuilder<V> addDeprecatedKey(String deprecatedKey) {
            this.deprecatedKeys = ConfigurationUtils.buildListIfNull(this.deprecatedKeys);
            ConfigurationUtils.addIfNotExistsWithEmptyCheck(this.deprecatedKeys, deprecatedKey, true);
            return ConfigItemBuilder.this;
        }

        public ConfigItemBuilder<V> addDeprecatedKeys(String[] deprecatedKeys) {
            if (!ConfigurationUtils.isEmptyArray(deprecatedKeys)) {
                for (String deprecatedKey : deprecatedKeys) {
                    this.addDeprecatedKey(deprecatedKey);
                }
            }
            return ConfigItemBuilder.this;
        }

        public ConfigItemBuilder<V> addDeprecatedKeys(List<String> deprecatedKeys) {
            if (deprecatedKeys != null && deprecatedKeys.size() > 0) {
                for (String deprecatedKey : deprecatedKeys) {
                    ConfigurationUtils.addIfNotExistsWithEmptyCheck(this.deprecatedKeys, deprecatedKey, true);
                }
            }
            return ConfigItemBuilder.this;
        }

        public ConfigItemBuilder<V> valueConverter(Converter<Object, V> converter) {
            this.valueConverter = ConfigurationUtils.checkNotNull(converter);
            return ConfigItemBuilder.this;
        }

        public ConfigItemBuilder<V> description(String description) {
            this.description = ConfigurationUtils.checkNotNull(description);
            return ConfigItemBuilder.this;
        }

        private void postCheckBeforeBuild() {
            if (this.valueClass == null) {
                String errMsg = "valueClass must set in ConfigItemBuilder.nonDefaultValue" +
                        " or ConfigItemBuilder.valueClass method.";
                LOGGER.error(errMsg);
                throw new ConfigurationException(errMsg);
            }
        }

        public ConfigItem<V> build() {
            this.postCheckBeforeBuild();
            return new ConfigItem<>(this.key, this.defaultValue, this.valueClass, this.deprecatedKeys,
                    this.description, this.valueConverter);
        }
    }

}