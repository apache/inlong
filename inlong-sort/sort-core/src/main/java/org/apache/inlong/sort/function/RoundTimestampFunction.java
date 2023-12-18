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

package org.apache.inlong.sort.function;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Round timestamp and output formatted timestamp.
 */
public class RoundTimestampFunction extends ScalarFunction {

    private static final long serialVersionUID = 1L;

    public static final Logger LOG = LoggerFactory.getLogger(RoundTimestampFunction.class);
    public static final ZoneId DEFAULT_ZONE = ZoneId.systemDefault();
    private transient Map<String, DateTimeFormatter> formatters;

    public void open(FunctionContext context) throws Exception {
        super.open(context);
        formatters = new HashMap<>();
    }

    /**
     * Round timestamp and output formatted timestamp.
     * For example, if the input timestamp is 1702610371(s), the roundTime is 600(s), and the format is "yyyyMMddHHmm",
     * the formatted timestamp is "2023121510".
     *
     * @param timestamp The input timestamp in seconds.
     * @param roundTime The round time in seconds.
     * @param format The format of the output timestamp.
     * @return The formatted timestamp.
     */
    public String eval(Long timestamp, Long roundTime, String format) {
        try {
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochSecond(timestamp - timestamp % roundTime),
                    DEFAULT_ZONE);
            DateTimeFormatter formatter = formatters.get(format);
            if (formatter == null) {
                formatter = DateTimeFormatter.ofPattern(format);
                formatters.put(format, formatter);
            }
            return dateTime.format(formatter);
        } catch (Exception e) {
            LOG.error("get formatted timestamp error, timestamp: {}, roundTime: {},format: {}",
                    timestamp, roundTime, format, e);
            return null;
        }
    }

}
