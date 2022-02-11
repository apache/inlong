/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.tubemq.corebase.metric;

import java.util.Map;

/**
 * An interface for metric item set.
 */
public interface MetricSet {
    /**
     * Get the current recorded values.
     *
     * @param keyValMap     the read result, the key is metric item's full name
     */
    void getValue(Map<String, Long> keyValMap);

    /**
     * Get the current recorded values.
     *
     * @param strBuff       string buffer, json format
     */
    void getValue(StringBuilder strBuff);

    /**
     * Get the current recorded values and reset to zero.
     *
     * @param keyValMap     the read result, the key is metric item's full name
     */
    void snapShort(Map<String, Long> keyValMap);

    /**
     * Get the current recorded values and reset to zero.
     *
     * @param strBuff       string buffer, json format
     */
    void snapShort(StringBuilder strBuff);
}
