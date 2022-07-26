/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.inlong.sort.iceberg.flink;

import java.util.Map;

public class CompactTableProperties {
    public static final String COMPACT_ENABLED = "write.compact.enable";
    public static final boolean COMPACT_ENABLED_DEFAULT = false;

    public static final String COMPACT_INTERVAL = "write.compact.snapshot.interval";
    public static final int COMPACT_INTERVAL_DEFAULT = 5;

    public static final String COMPACT_RESOUCE_POOL = "write.compact.resource.name";
    public static final String COMPACT_RESOUCE_POOL_DEFAULT = "default";

    public static CompactTableProperties build(Map<String, String> tableProperties) {
        return new CompactTableProperties();
    }
}
