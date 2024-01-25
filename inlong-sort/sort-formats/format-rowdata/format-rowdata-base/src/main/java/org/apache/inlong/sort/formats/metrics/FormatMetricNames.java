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

package org.apache.inlong.sort.formats.metrics;

/**
 * Collection of metric names.
 */
public class FormatMetricNames {

    public static final String GAUGE_EVENT_TIME_DELAY_MILLIS = "eventDelayTime";

    public static final String COUNTER_NUM_RECORDS_SERIALIZE_ERROR = "numRecordsSerializeError";

    public static final String GAUGE_NUM_RECORDS_SERIALIZE_ERROR_DIFF = "numRecordsSerializeErrorDiff";

    public static final String COUNTER_NUM_RECORDS_SERIALIZE_ERROR_IGNORED = "numRecordsSerializeErrorIgnored";

    public static final String GAUGE_NUM_RECORDS_SERIALIZE_ERROR_IGNORED_DIFF = "numRecordsSerializeErrorIgnoredDiff";

    public static final String COUNTER_NUM_RECORDS_DESERIALIZE_ERROR = "numRecordsDeserializeError";

    public static final String GAUGE_NUM_RECORDS_DESERIALIZE_ERROR_DIFF = "numRecordsDeserializeErrorDiff";

    public static final String COUNTER_NUM_RECORDS_DESERIALIZE_ERROR_IGNORED = "numRecordsDeserializeErrorIgnored";

    public static final String GAUGE_NUM_RECORDS_DESERIALIZE_ERROR_IGNORED_DIFF =
            "numRecordsDeserializeErrorIgnoredDiff";
}
