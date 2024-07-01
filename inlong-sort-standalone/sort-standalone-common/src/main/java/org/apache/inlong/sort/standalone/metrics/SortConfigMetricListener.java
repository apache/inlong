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

package org.apache.inlong.sort.standalone.metrics;

import org.apache.inlong.common.pojo.sort.SortConfig;

import org.apache.flume.conf.Configurable;

import java.util.Collection;

public interface SortConfigMetricListener extends Configurable {

    void reportOffline(SortConfig sortConfig);
    void reportOnline(SortConfig sortConfig);
    void reportUpdate(SortConfig sortConfig);
    void reportParseFail(String dataflowId);
    void reportRequestConfigFail();
    void reportDecompressFail();
    void reportCheckFail();
    void reportRequestNoUpdate();
    void reportRequestUpdate();
    void reportMissInSortClusterConfig(Collection<String> dataflows);
    void reportMissInSortConfig(Collection<String> dataflows);
}
