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

package org.apache.inlong.agent.plugin.utils;

import io.cloudevents.CloudEvent;
import org.apache.inlong.agent.message.DefaultMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Convert data with cloudEvents format into other format
 */
public class CloudEventsConverterUtil {

    private CloudEventsConverterUtil() {

    }

    /**
     * Convert data with cloudEvents format into DefaultMessage
     *
     * @param data data with cloudEvents format
     * @param header the attributes of message
     * @return data with DefaultMessage format
     */
    public static DefaultMessage convertToDefaultMessage(CloudEvent data, Map<String, String> header) {
        header.put("cloudEvents.id", data.getId());
        header.put("cloudEvents.type", data.getType());
        header.put("cloudEvents.source", data.getSource().toString());
        return new DefaultMessage(Objects.requireNonNull(data.getData()).toBytes(), header);
    }

    /**
     * Convert data with cloudEvents format into DefaultMessage
     *
     * @param data data with cloudEvents format
     * @return data with DefaultMessage format
     */
    public static DefaultMessage convertToDefaultMessage(CloudEvent data) {
        Map<String, String> header = new HashMap<>();
        header.put("cloudEvents.id", data.getId());
        header.put("cloudEvents.type", data.getType());
        header.put("cloudEvents.source", data.getSource().toString());
        return new DefaultMessage(Objects.requireNonNull(data.getData()).toBytes(), header);
    }
}
