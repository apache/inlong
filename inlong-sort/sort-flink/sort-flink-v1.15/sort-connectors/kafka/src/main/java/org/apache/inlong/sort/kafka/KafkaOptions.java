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

package org.apache.inlong.sort.kafka;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Option utils for Kafka table source sink. */
public class KafkaOptions {

    private KafkaOptions() {
    }

    public static final ConfigOption<String> SINK_MULTIPLE_PARTITION_PATTERN =
            ConfigOptions.key("sink.multiple.partition-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "option 'sink.multiple.partition-pattern' used either when the partitioner is raw-hash, or when passing in designated partition field names for custom field partitions");

    public static final ConfigOption<String> SINK_FIXED_IDENTIFIER =
            ConfigOptions.key("sink.fixed.identifier")
                    .stringType()
                    .defaultValue("-1");


    // --------------------------------------------------------------------------------------------
    // Sink specific options
    // --------------------------------------------------------------------------------------------
    public static final ConfigOption<Boolean> KAFKA_IGNORE_ALL_CHANGELOG =
            ConfigOptions.key("sink.ignore.changelog")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Regard upsert delete as insert kind.");

}
