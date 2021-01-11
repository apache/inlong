/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.corebase.utils;

import org.apache.tubemq.corebase.TBaseConstants;


public class SettingValidUtils {

    // get the middle data between min, max, and data
    public static int mid(int data, int min, int max) {
        return Math.max(min, Math.min(max, data));
    }

    public static long mid(long data, long min, long max) {
        return Math.max(min, Math.min(max, data));
    }

    public static int validAndGetMaxMsgSize(int inMaxMsgSize) {
        return mid(inMaxMsgSize,
            TBaseConstants.META_MAX_MESSAGE_DATA_SIZE,
            TBaseConstants.META_MAX_MESSAGE_DATA_SIZE_UPPER_LIMIT);
    }
}
