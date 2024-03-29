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

package org.apache.inlong.sort.iceberg;

import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.flink.util.FlinkPackage;

/**
 * Copy from iceberg-flink:iceberg-flink-1.15:1.3.1
 */
class FlinkEnvironmentContext {

    private FlinkEnvironmentContext() {
    }

    public static void init() {
        EnvironmentContext.put(EnvironmentContext.ENGINE_NAME, "flink");
        EnvironmentContext.put(EnvironmentContext.ENGINE_VERSION, FlinkPackage.version());
    }
}
