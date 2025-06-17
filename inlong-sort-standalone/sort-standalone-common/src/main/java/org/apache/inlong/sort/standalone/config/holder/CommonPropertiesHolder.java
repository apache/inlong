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

package org.apache.inlong.sort.standalone.config.holder;

import org.apache.inlong.sort.standalone.config.loader.ClassResourceCommonPropertiesLoader;
import org.apache.inlong.sort.standalone.config.loader.CommonPropertiesLoader;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flume.Context;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * CommonPropertiesHolder
 */
public class CommonPropertiesHolder {

    public static final Logger LOG = InlongLoggerFactory.getLogger(CommonPropertiesHolder.class);
    public static final String DEFAULT_LOADER = ClassResourceCommonPropertiesLoader.class.getName();
    public static final String KEY_COMMON_PROPERTIES = "common_properties_loader";
    public static final String KEY_CLUSTER_ID = "clusterId";
    public static final String KEY_SORT_SOURCE_ACKPOLICY = "sortSource.ackPolicy";
    public static final String KEY_USE_UNIFIED_CONFIGURATION = "useUnifiedConfiguration";

    public static final String KEY_MAX_SENDFAIL_TIMES = "maxSendFailTimes";
    public static final int DEFAULT_MAX_SENDFAIL_TIMES = 0;
    public static final String KEY_SENDFAIL_PAUSE_CONSUMER_MIN = "sendFailPauseConsumerMin";
    public static final int DEFAULT_SENDFAIL_PAUSE_CONSUMER_MIN = 10;

    private static Map<String, String> props;
    private static Context context;

    private static long auditFormatInterval = 60000L;
    private static AckPolicy ackPolicy;

    private static AtomicInteger maxSendFailTimes = new AtomicInteger(-1);
    private static AtomicInteger sendFailPauseConsumerMinutes = new AtomicInteger(-1);

    /**
     * init
     */
    private static void init() {
        synchronized (KEY_COMMON_PROPERTIES) {
            if (props == null) {
                props = new ConcurrentHashMap<>();
                String loaderClassName = System.getenv(KEY_COMMON_PROPERTIES);
                loaderClassName = (loaderClassName == null) ? DEFAULT_LOADER : loaderClassName;
                try {
                    Class<?> loaderClass = ClassUtils.getClass(loaderClassName);
                    Object loaderObject = loaderClass.getDeclaredConstructor().newInstance();
                    if (loaderObject instanceof CommonPropertiesLoader) {
                        CommonPropertiesLoader loader = (CommonPropertiesLoader) loaderObject;
                        props.putAll(loader.load());
                        LOG.info("loaderClass:{},properties:{}", loaderClassName, props);
                        CommonPropertiesHolder.auditFormatInterval = NumberUtils
                                .toLong(CommonPropertiesHolder.getString("auditFormatInterval"), 60000L);
                        String strAckPolicy = CommonPropertiesHolder.getString(KEY_SORT_SOURCE_ACKPOLICY,
                                AckPolicy.COUNT.name());
                        CommonPropertiesHolder.ackPolicy = AckPolicy.getAckPolicy(strAckPolicy);
                    }
                } catch (Throwable t) {
                    LOG.error("Fail to init CommonPropertiesLoader,loaderClass:{},error:{}",
                            loaderClassName, t.getMessage());
                    LOG.error(t.getMessage(), t);
                }
                context = new Context(props);
            }
        }
    }

    /**
     * get props
     * 
     * @return the props
     */
    public static Map<String, String> get() {
        if (props != null) {
            return props;
        }
        init();
        return props;
    }

    /**
     * get context
     *
     * @return the context
     */
    public static Context getContext() {
        if (context != null) {
            return context;
        }
        init();
        return context;
    }

    /**
     * Gets value mapped to key, returning defaultValue if unmapped.
     * 
     * @param  key          to be found
     * @param  defaultValue returned if key is unmapped
     * @return              value associated with key
     */
    public static String getString(String key, String defaultValue) {
        return get().getOrDefault(key, defaultValue);
    }

    /**
     * Gets value mapped to key, returning null if unmapped.
     * 
     * @param  key to be found
     * @return     value associated with key or null if unmapped
     */
    public static String getString(String key) {
        return get().get(key);
    }

    /**
     * Gets value mapped to key, returning defaultValue if unmapped.
     * 
     * @param  key          to be found
     * @param  defaultValue returned if key is unmapped
     * @return              value associated with key
     */
    public static Long getLong(String key, Long defaultValue) {
        return NumberUtils.toLong(get().get(key), defaultValue);
    }

    /**
     * Gets value mapped to key, returning null if unmapped.
     * 
     * @param  key to be found
     * @return     value associated with key or null if unmapped
     */
    public static Long getLong(String key) {
        String strValue = get().get(key);
        Long value = (strValue == null) ? null : NumberUtils.toLong(get().get(key));
        return value;
    }

    /**
     * getStringFromContext
     * 
     * @param  context
     * @param  key
     * @param  defaultValue
     * @return
     */
    public static String getStringFromContext(Context context, String key, String defaultValue) {
        String value = context.getString(key);
        value = (value != null) ? value : props.getOrDefault(key, defaultValue);
        return value;
    }

    /**
     * Gets value mapped to key, returning defaultValue if unmapped.
     * 
     * @param  key          to be found
     * @param  defaultValue returned if key is unmapped
     * @return              value associated with key
     */
    public static Integer getInteger(String key, Integer defaultValue) {
        String value = get().get(key);
        if (value != null) {
            return Integer.valueOf(Integer.parseInt(value.trim()));
        }
        return defaultValue;
    }

    public static Boolean getBoolean(String key, Boolean defaultValue) {
        String value = get().get(key);
        if (value != null) {
            return Boolean.valueOf(value.trim());
        }
        return defaultValue;
    }

    /**
     * Gets value mapped to key, returning null if unmapped.
     * <p>
     * Note that this method returns an object as opposed to a primitive. The configuration key requested may not be
     * mapped to a value and by returning the primitive object wrapper we can return null. If the key does not exist the
     * return value of this method is assigned directly to a primitive, a {@link NullPointerException} will be thrown.
     * </p>
     * 
     * @param  key to be found
     * @return     value associated with key or null if unmapped
     */
    public static Integer getInteger(String key) {
        return getInteger(key, null);
    }

    /**
     * getClusterId
     * 
     * @return
     */
    public static String getClusterId() {
        return getString(KEY_CLUSTER_ID);
    }

    /**
     * getAuditFormatInterval
     *
     * @return
     */
    public static long getAuditFormatInterval() {
        return auditFormatInterval;
    }

    /**
     * get ackPolicy
     * @return the ackPolicy
     */
    public static AckPolicy getAckPolicy() {
        return ackPolicy;
    }

    public static boolean useUnifiedConfiguration() {
        return getBoolean(KEY_USE_UNIFIED_CONFIGURATION, false);
    }

    public static int getMaxSendFailTimes() {
        int result = maxSendFailTimes.get();
        if (result >= 0) {
            return result;
        }
        int newResult = getInteger(KEY_MAX_SENDFAIL_TIMES, DEFAULT_MAX_SENDFAIL_TIMES);
        maxSendFailTimes.compareAndSet(result, newResult);
        return newResult;
    }

    public static int getSendFailPauseConsumerMinutes() {
        int result = sendFailPauseConsumerMinutes.get();
        if (result >= 0) {
            return result;
        }
        int newResult = getInteger(KEY_SENDFAIL_PAUSE_CONSUMER_MIN, DEFAULT_SENDFAIL_PAUSE_CONSUMER_MIN);
        sendFailPauseConsumerMinutes.compareAndSet(result, newResult);
        return newResult;
    }
}
