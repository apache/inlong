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

package org.apache.inlong.audit.utils;

import org.apache.commons.lang3.StringUtils;

public class DataUtils {

    private static final int MAX_AUDIT_ITEM_LENGTH = 256;

    /**
     * Checks if the timestamp is within the specified deviation range.
     *
     * @param dataTime  The timestamp to check.
     * @param deviation The allowed time deviation.
     * @return true if the timestamp is within the deviation range, false otherwise.
     */
    public static boolean isDataTimeValid(long dataTime, long deviation) {
        long currentTime = System.currentTimeMillis();
        long timeDiff = Math.abs(currentTime - dataTime);
        return timeDiff <= deviation;
    }

    /**
     * Checks if the audit item is valid.
     *
     * @param auditItem The audit item to check.
     * @return true if the audit item is blank or its length is less than the maximum length, false otherwise.
     */
    public static boolean isAuditItemValid(String auditItem) {
        return StringUtils.isBlank(auditItem) || auditItem.length() < MAX_AUDIT_ITEM_LENGTH;
    }
}