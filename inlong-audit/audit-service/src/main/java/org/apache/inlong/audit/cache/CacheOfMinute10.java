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

package org.apache.inlong.audit.cache;

import org.apache.inlong.audit.config.Configuration;
import org.apache.inlong.audit.entities.AuditCycle;

/**
 * Cache Of minute 10 ,for minute 10 openapi
 */
public class CacheOfMinute10 extends AbstractCache {

    private static volatile CacheOfMinute10 cacheOfMinute10 = null;

    private CacheOfMinute10() {
        super(AuditCycle.MINUTE_10);
    }
    public static CacheOfMinute10 getInstance() {
        if (cacheOfMinute10 == null) {
            synchronized (Configuration.class) {
                if (cacheOfMinute10 == null) {
                    cacheOfMinute10 = new CacheOfMinute10();
                }
            }
        }
        return cacheOfMinute10;
    }
}
