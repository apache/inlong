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

package org.apache.inlong.sdk.transform.process;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transform context.
 *
 * <p>configuration</p> is the global configuration when init transform processor
 * <p>extParams</p> is the ext params of each data
 * <p>runtimeParams</p> is the runtime outputs when processing by each component
 *
 * The priority is runtimeParams > extParams > configuration.
 */
public class Context {

    private final Map<String, Object> configuration;
    private final Map<String, Object> extParams;
    private final Map<String, Object> runtimeParams;

    public Context(Map<String, Object> configuration, Map<String, Object> extParams) {
        this.configuration = configuration;
        this.extParams = extParams;
        this.runtimeParams = new ConcurrentHashMap<>();
    }

    public Object put(String key, Object value) {
        return runtimeParams.put(key, value);
    }

    public Object get(String key) {
        Object obj = runtimeParams.get(key);
        if (obj != null) {
            return obj;
        }
        obj = extParams.get(key);
        if (obj != null) {
            return obj;
        }
        return configuration.get(key);
    }

    public String getStringOrDefault(String key, String defaultValue) {
        String str = getString(key);
        if (str == null) {
            return defaultValue;
        }
        return str;
    }

    public String getString(String key) {
        Object obj = this.get(key);
        if (obj != null) {
            return obj.toString();
        }
        return null;
    }

    public Integer getInteger(String key) {
        Object obj = this.get(key);
        if (obj != null) {
            return Integer.getInteger(obj.toString());
        }
        return null;
    }

    public Long getLong(String key) {
        Object obj = this.get(key);
        if (obj != null) {
            return Long.getLong(obj.toString());
        }
        return null;
    }

    public Map<String, Object> getRuntimeParams() {
        return runtimeParams;
    }
}
