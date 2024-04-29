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

package org.apache.inlong.sdk.transform.pojo;

import java.util.Locale;

/**
 * ProtocolType
 */
public enum ProtocolType {

    CSV("csv"), KV("kv"), PB("pb"), JSON("json"), UNKNOWN("n");

    private final String type;

    ProtocolType(String type) {
        this.type = type;
    }

    public static ProtocolType forType(String type) {
        for (ProtocolType dataType : values()) {
            if (dataType.getType().equals(type.toLowerCase(Locale.ROOT))) {
                return dataType;
            }
        }
        throw new IllegalArgumentException("Unsupported protocol type for " + type);
    }

    public static ProtocolType convert(String value) {
        for (ProtocolType v : values()) {
            if (v.getType().equals(value.toLowerCase(Locale.ROOT))) {
                return v;
            }
        }
        return UNKNOWN;
    }

    @Override
    public String toString() {
        return this.name() + ":" + this.type;
    }

    public String getType() {
        return type;
    }
}
