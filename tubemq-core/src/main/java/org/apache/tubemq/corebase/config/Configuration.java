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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

    /**
     * Stores the concrete key/value pairs of this configuration object.
     */
    protected final HashMap<String, Object> internalData;

    // --------------------------------------------------------------------------------------------

    public Configuration() {
        this.internalData = new HashMap<>();
    }

    public Configuration(Configuration other) {
        this.internalData = new HashMap<>(other.internalData);
    }

    public static Configuration fromMap(Map<String, String> map) {
        final Configuration configuration = new Configuration();
        for (String key : map.keySet()) {
            configuration.setString(key, map.get(key));
        }
        return configuration;
    }

    public <T> T get(ConfigItem<T> option) {
        Object rawValue = getRawValueFromOption(option);
        if (rawValue == null && option.isDefaulted()) {
            return option.getDefaultValue();
        }
        if (rawValue != null) {
            return option.doConvert(rawValue);
        }
        return null;
    }

    public Object get(String key) {
        return getRawValue(key);
    }

    public Object get(String key, Object defaultValue) {
        Object value = get(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    public <T> T get(ConfigItem<T> option, T overrideDefaultValue) {
        Object rawValue = getRawValueFromOption(option);
        if (rawValue == null) {
            return overrideDefaultValue;
        }
        return option.doConvert(rawValue);
    }

    public String getString(String key, String defaultValue) {
        Object rawValue = getRawValue(key);
        if (rawValue == null) {
            return defaultValue;
        }
        return String.valueOf(rawValue);
    }

    public String getString(ConfigItem<String> configItem) {
        Object rawValue = getRawValue(configItem.getKey());
        if (rawValue == null && configItem.isDefaulted()) {
            return configItem.getDefaultValue();
        }
        return configItem.doConvert(rawValue);
    }

    /**
     * Returns the value associated with the given config option as a string. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configItem The configuration option
     * @return the (default) value associated with the given config option
     */
    public String getString(ConfigItem<String> configItem, String overrideDefault) {
        Object rawValue = getRawValueFromOption(configItem);
        return rawValue == null ? overrideDefault : String.valueOf(configItem.doConvert(rawValue));
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key   the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setString(String key, String value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param option   the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setString(ConfigItem<String> option, String value) {
        setValueInternal(option.getKey(), value);
    }

    public Integer getInteger(String key, int defaultValue) {
        Object rawObj = getRawValue(key);
        return rawObj == null ? defaultValue : Integer.parseInt(String.valueOf(rawObj));
    }

    /**
     * Returns the value associated with the given config option as an integer.
     *
     * @param configItem The configuration option
     * @return the (default) value associated with the given config option
     */
    public Integer getInteger(ConfigItem<Integer> configItem) {
        Object rawObj = getRawValueFromOption(configItem);
        if (rawObj == null && configItem.isDefaulted()) {
            return configItem.getDefaultValue();
        }
        if (rawObj != null) {
            return configItem.doConvert(rawObj);
        }
        return null;
    }

    /**
     * Returns the value associated with the given config option as an integer. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configItem    The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public Integer getInteger(ConfigItem<Integer> configItem, int overrideDefault) {
        return getInteger(configItem) == null ? overrideDefault : getInteger(configItem);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key   the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setInteger(String key, int value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key   the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setInteger(ConfigItem<Integer> key, int value) {
        setValueInternal(key.getKey(), value);
    }

    /**
     * Returns the value associated with the given key as a long.
     *
     * @param key          the key pointing to the associated value
     * @param defaultValue the default value which is returned in case there is no value associated
     *                     with the given key
     * @return the (default) value associated with the given key
     * @deprecated use {@link #getLong(ConfigItem, long)} or {@link #getRawValue(String)}}
     */
    public Long getLong(String key, long defaultValue) {
        Object raw = getRawValue(key);
        return raw == null ? defaultValue : Long.parseLong(String.valueOf(raw));
    }

    /**
     * Returns the value associated with the given config option as a long integer.
     *
     * @param configItem The configuration option
     * @return the (default) value associated with the given config option
     */
    public Long getLong(ConfigItem<Long> configItem) {
        Object raw = getRawValueFromOption(configItem);
        if (raw == null && configItem.isDefaulted()) {
            return configItem.getDefaultValue();
        }
        if (raw != null) {
            return Long.parseLong(String.valueOf(raw));
        }
        return null;
    }

    /**
     * Returns the value associated with the given config option as a long integer. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configItem    The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public Long getLong(ConfigItem<Long> configItem, long overrideDefault) {
        return getLong(configItem) == null ? overrideDefault : getLong(configItem);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key   the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setLong(String key, long value) {
        setValueInternal(key, value);
    }

    public void setLong(ConfigItem<Long> key, long value) {
        setValueInternal(key.getKey(), value);
    }

    public Boolean getBoolean(String key, boolean defaultValue) {
        Object raw = getRawValue(key);
        if (raw == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(String.valueOf(raw));
    }

    /**
     * Returns the value associated with the given config option as a boolean.
     *
     * @param configItem The configuration option
     * @return the (default) value associated with the given config option
     */
    public Boolean getBoolean(ConfigItem<Boolean> configItem) {
        return get(configItem);
    }

    /**
     * Returns the value associated with the given config option as a boolean. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configItem    The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public Boolean getBoolean(ConfigItem<Boolean> configItem, boolean overrideDefault) {
        return get(configItem, overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key   the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setBoolean(String key, boolean value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key   the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setBoolean(ConfigItem<Boolean> key, boolean value) {
        setValueInternal(key.getKey(), value);
    }

    /**
     * Returns the value associated with the given key as a float.
     *
     * @param key          the key pointing to the associated value
     * @param defaultValue the default value which is returned in case there is no value associated
     *                     with the given key
     * @return the (default) value associated with the given key
     */
    public Float getFloat(String key, float defaultValue) {
        Object raw = getRawValue(key);
        if (raw == null) {
            return defaultValue;
        }
        return  Float.parseFloat(String.valueOf(raw));
    }

    /**
     * Returns the value associated with the given config option as a float.
     *
     * @param configItem The configuration option
     * @return the (default) value associated with the given config option
     */
    public Float getFloat(ConfigItem<Float> configItem) {
        return get(configItem);
    }

    /**
     * Returns the value associated with the given config option as a float. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configItem    The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public Float getFloat(ConfigItem<Float> configItem, float overrideDefault) {
        return get(configItem, overrideDefault);
    }

    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key   the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setFloat(String key, float value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param key   the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setFloat(ConfigItem<Float> key, float value) {
        setValueInternal(key.getKey(), value);
    }

    /**
     * Returns the value associated with the given key as a double.
     *
     * @param key          the key pointing to the associated value
     * @param defaultValue the default value which is returned in case there is no value associated
     *                     with the given key
     * @return the (default) value associated with the given key
     */
    public Double getDouble(String key, double defaultValue) {
        Object raw = getRawValue(key);
        return raw == null ? defaultValue : Double.parseDouble(String.valueOf(raw));
    }

    /**
     * Returns the value associated with the given config option as a {@code double}.
     *
     * @param configItem The configuration option
     * @return the (default) value associated with the given config option
     */
    public Double getDouble(ConfigItem<Double> configItem) {
        return get(configItem);
    }

    /**
     * Returns the value associated with the given config option as a {@code double}. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configItem    The configuration option
     * @param overrideDefault The value to return if no value was mapper for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public Double getDouble(ConfigItem<Double> configItem, double overrideDefault) {
        return get(configItem, overrideDefault);
    }



    /**
     * Adds the given key/value pair to the configuration object.
     *
     * @param key   the key of the key/value pair to be added
     * @param value the value of the key/value pair to be added
     */
    public void setDouble(String key, double value) {
        setValueInternal(key, value);
    }

    /**
     * Adds the given value to the configuration object. The main key of the config option will be
     * used to map the value.
     *
     * @param option   the option specifying the key to be added
     * @param value the value of the key/value pair to be added
     */
    public void setDouble(ConfigItem<Double> option, double value) {
        setValueInternal(option.getKey(), value);
    }

    /**
     * Adds the given byte array to the configuration object. If key is <code>null</code> then
     * nothing is added.
     *
     * @param key   The key under which the bytes are added.
     * @param bytes The bytes to be added.
     */
    public void setBytes(String key, byte[] bytes) {
        setValueInternal(key, bytes);
    }

    /**
     * Returns the value associated with the given config option as a string.
     *
     * @param configItem The configuration option
     * @return the (default) value associated with the given config option
     */
    public String getValue(ConfigItem<?> configItem) {
        Object rawObject = getRawValueFromOption(configItem);
        if (rawObject == null && configItem.isDefaulted()) {
            return String.valueOf(configItem.getDefaultValue());
        }
        if (rawObject == null) {
            return null;
        }
        return String.valueOf(configItem.doConvert(rawObject));
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Returns the keys of all key/value pairs stored inside this configuration object.
     *
     * @return the keys of all key/value pairs stored inside this configuration object
     */
    public Set<String> keySet() {
        synchronized (this.internalData) {
            return new HashSet<>(this.internalData.keySet());
        }
    }

    /**
     * Adds all entries in this {@code Configuration} to the given {@link Properties}.
     */
    public void addAllToProperties(Properties props) {
        synchronized (this.internalData) {
            for (Map.Entry<String, Object> entry : this.internalData.entrySet()) {
                props.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void addAll(Configuration other) {
        synchronized (this.internalData) {
            synchronized (other.internalData) {
                this.internalData.putAll(other.internalData);
            }
        }
    }

    /**
     * Adds all entries from the given configuration into this configuration. The keys are prepended
     * with the given prefix.
     *
     * @param other  The configuration whose entries are added to this configuration.
     * @param prefix The prefix to prepend.
     */
    public void addAll(Configuration other, String prefix) {
        final StringBuilder bld = new StringBuilder();
        bld.append(prefix);
        final int pl = bld.length();

        synchronized (this.internalData) {
            synchronized (other.internalData) {
                for (Map.Entry<String, Object> entry : other.internalData.entrySet()) {
                    bld.setLength(pl);
                    bld.append(entry.getKey());
                    this.internalData.put(bld.toString(), entry.getValue());
                }
            }
        }
    }

    @Override
    public Configuration clone() {
        Configuration config = new Configuration();
        config.addAll(this);
        return config;
    }

    /**
     * Checks whether there is an entry with the specified key.
     *
     * @param key key of entry
     * @return true if the key is stored, false otherwise
     */
    public boolean containsKey(String key) {
        synchronized (this.internalData) {
            return this.internalData.containsKey(key);
        }
    }

    public boolean contains(ConfigItem<?> configItem) {
        synchronized (this.internalData) {
            // first try the current key
            if (this.internalData.containsKey(configItem.getKey())) {
                return true;
            } else if (!ConfigurationUtils.isEmptyList(configItem.getDeprecatedKeys())) {
                // try the fallback keys
                for (String fallbackKey : configItem.getDeprecatedKeys()) {
                    if (this.internalData.containsKey(fallbackKey)) {
                        loggingFallback(fallbackKey, configItem);
                        return true;
                    }
                }
            }
            return false;
        }
    }

    public <T> Configuration set(String key, T value) {
        setValueInternal(key, value);
        return this;
    }

    public <T> Configuration set(ConfigItem<T> option, T value) {
        setValueInternal(option.getKey(), value);
        return this;
    }

    public Map<String, String> toMap() {
        synchronized (this.internalData) {
            Map<String, String> ret = new HashMap<>(this.internalData.size());
            for (Map.Entry<String, Object> entry : internalData.entrySet()) {
                ret.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            return ret;
        }
    }

    /**
     * Removes given config option from the configuration.
     *
     * @param configItem config option to remove
     * @param <T>          Type of the config option
     * @return true is config has been removed, false otherwise
     */
    public <T> boolean removeConfig(ConfigItem<T> configItem) {
        synchronized (this.internalData) {
            // try the current key
            Object oldValue = this.internalData.remove(configItem.getKey());
            if (oldValue == null && !ConfigurationUtils.isEmptyList(configItem.getDeprecatedKeys())) {
                for (String deprecatedKey : configItem.getDeprecatedKeys()) {
                    oldValue = this.internalData.remove(deprecatedKey);
                    if (oldValue != null) {
                        loggingFallback(deprecatedKey, configItem);
                        return true;
                    }
                }
                return false;
            }
            return true;
        }
    }

    // --------------------------------------------------------------------------------------------

    <T> void setValueInternal(String key, T value) {
        if (key == null) {
            throw new NullPointerException("Key must not be null.");
        }
        if (value == null) {
            throw new NullPointerException("Value must not be null.");
        }

        synchronized (this.internalData) {
            this.internalData.put(key, value);
        }
    }

    private Object getRawValue(String key) {
        ConfigurationUtils.checkNotNull(key);
        ConfigurationUtils.checkNotEmpty(key);
        synchronized (this.internalData) {
            return this.internalData.get(key);
        }
    }

    private Object getRawValueFromOption(ConfigItem<?> configItem) {
        // first try the current key
        Object rawValue = getRawValue(configItem.getKey());

        if (rawValue == null) {
            if (!ConfigurationUtils.isEmptyList(configItem.getDeprecatedKeys())) {
                for (String deprecatedKey : configItem.getDeprecatedKeys()) {
                    rawValue = getRawValue(deprecatedKey);
                    if (rawValue != null) {
                        loggingFallback(deprecatedKey, configItem);
                        return rawValue;
                    }
                }
            }
        }
        return rawValue;
    }

    private void loggingFallback(String fallbackKey, ConfigItem<?> configItem) {
        LOGGER.warn(
                "Config uses deprecated configuration key '{}' instead of proper key '{}'",
                fallbackKey,
                configItem.getKey());
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        int hash = 0;
        for (String s : this.internalData.keySet()) {
            hash ^= s.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof Configuration) {
            Map<String, Object> otherConf = ((Configuration) obj).internalData;
            for (Map.Entry<String, Object> e : this.internalData.entrySet()) {
                Object thisVal = e.getValue();
                Object otherVal = otherConf.get(e.getKey());

                if (!thisVal.getClass().equals(byte[].class)) {
                    if (!thisVal.equals(otherVal)) {
                        return false;
                    }
                } else if (otherVal.getClass().equals(byte[].class)) {
                    if (!Arrays.equals((byte[]) thisVal, (byte[]) otherVal)) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return this.internalData.toString();
    }
}

