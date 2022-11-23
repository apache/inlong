/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.inlong.sort.base;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.inlong.sort.base.sink.SchemaUpdateExceptionPolicy;

/**
 * connector base option constant
 */
public final class Constants {

    /**
     * constants for metrics
     */
    public static final String DIRTY_BYTES = "dirtyBytes";

    public static final String DIRTY_RECORDS = "dirtyRecords";

    public static final String NUM_BYTES_OUT = "numBytesOut";

    public static final String NUM_RECORDS_OUT = "numRecordsOut";

    public static final String NUM_BYTES_OUT_FOR_METER = "numBytesOutForMeter";

    public static final String NUM_RECORDS_OUT_FOR_METER = "numRecordsOutForMeter";

    public static final String NUM_BYTES_OUT_PER_SECOND = "numBytesOutPerSecond";

    public static final String NUM_RECORDS_OUT_PER_SECOND = "numRecordsOutPerSecond";

    public static final String NUM_RECORDS_IN = "numRecordsIn";

    public static final String NUM_BYTES_IN = "numBytesIn";

    public static final String NUM_RECORDS_IN_FOR_METER = "numRecordsInForMeter";

    public static final String NUM_BYTES_IN_FOR_METER = "numBytesInForMeter";

    public static final String NUM_BYTES_IN_PER_SECOND = "numBytesInPerSecond";

    public static final String NUM_RECORDS_IN_PER_SECOND = "numRecordsInPerSecond";
    /**
     * Time span in seconds
     */
    public static final Integer TIME_SPAN_IN_SECONDS = 60;
    /**
     * Stream id used in inlong metric
     */
    public static final String STREAM_ID = "streamId";
    /**
     * Group id used in inlong metric
     */
    public static final String GROUP_ID = "groupId";
    /**
     * Node id used in inlong metric
     */
    public static final String NODE_ID = "nodeId";
    /**
     * It is used for inlong.metric
     */
    public static final String DELIMITER = "&";

    // sort received successfully
    public static final Integer AUDIT_SORT_INPUT = 7;

    // sort send successfully
    public static final Integer AUDIT_SORT_OUTPUT = 8;

    public static final String INLONG_METRIC_STATE_NAME = "inlong-metric-states";

    /**
     * It is used for jdbc url filter for avoiding url attack
     * see also in https://su18.org/post/jdbc-connection-url-attack/
     */
    public static final String AUTO_DESERIALIZE = "autoDeserialize";

    public static final String AUTO_DESERIALIZE_TRUE = "autoDeserialize=true";

    public static final String AUTO_DESERIALIZE_FALSE = "autoDeserialize=false";

    public static final ConfigOption<String> INLONG_METRIC =
            ConfigOptions.key("inlong.metric.labels")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("INLONG metric labels, format is 'key1=value1&key2=value2',"
                            + "default is 'groupId=xxx&streamId=xxx&nodeId=xxx'");

    public static final ConfigOption<String> INLONG_AUDIT =
            ConfigOptions.key("metrics.audit.proxy.hosts")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Audit proxy host address for reporting audit metrics. \n"
                            + "e.g. 127.0.0.1:10081,0.0.0.1:10081");

    public static final ConfigOption<Boolean> IGNORE_ALL_CHANGELOG =
            ConfigOptions.key("sink.ignore.changelog")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Regard upsert delete as insert kind.");

    public static final ConfigOption<String> SINK_MULTIPLE_FORMAT =
            ConfigOptions.key("sink.multiple.format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The format of multiple sink, it represents the real format of the raw binary data");

    public static final ConfigOption<String> SINK_MULTIPLE_DATABASE_PATTERN =
            ConfigOptions.key("sink.multiple.database-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The option 'sink.multiple.database-pattern' "
                            + "is used extract database name from the raw binary data, "
                            + "this is only used in the multiple sink writing scenario.");

    public static final ConfigOption<Boolean> SOURCE_MULTIPLE_ENABLE =
            ConfigOptions.key("source.multiple.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether enable migrate multiple databases");

    public static final ConfigOption<String> SINK_MULTIPLE_TABLE_PATTERN =
            ConfigOptions.key("sink.multiple.table-pattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The option 'sink.multiple.table-pattern' "
                            + "is used extract table name from the raw binary data, "
                            + "this is only used in the multiple sink writing scenario.");

    public static final ConfigOption<Boolean> SINK_MULTIPLE_ENABLE =
            ConfigOptions.key("sink.multiple.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("The option 'sink.multiple.enable' "
                            + "is used to determine whether to support multiple sink writing, default is 'false'.");

    public static final ConfigOption<SchemaUpdateExceptionPolicy> SINK_MULTIPLE_SCHEMA_UPDATE_POLICY =
            ConfigOptions.key("sink.multiple.schema-update.policy")
                    .enumType(SchemaUpdateExceptionPolicy.class)
                    .defaultValue(SchemaUpdateExceptionPolicy.TRY_IT_BEST)
                    .withDescription("The action to deal with schema update in multiple sink.");

    public static final ConfigOption<Boolean> SINK_MULTIPLE_IGNORE_SINGLE_TABLE_ERRORS =
            ConfigOptions.key("sink.multiple.ignore-single-table-errors")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether ignore the single table erros when multiple sink writing scenario.");

    public static final ConfigOption<Boolean> SINK_MULTIPLE_PK_AUTO_GENERATED =
            ConfigOptions.key("sink.multiple.pk-auto-generated")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether generated pk fields as whole data when source table does not have a "
                            + "primary key.");

    public static final ConfigOption<Boolean> SINK_MULTIPLE_TYPE_MAP_COMPATIBLE_WITH_SPARK =
            ConfigOptions.key("sink.multiple.typemap-compatible-with-spark")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Because spark do not support iceberg data type: `timestamp without time zone` and"
                            + "`time`, so type conversions must be mapped to types supported by spark.");
}
