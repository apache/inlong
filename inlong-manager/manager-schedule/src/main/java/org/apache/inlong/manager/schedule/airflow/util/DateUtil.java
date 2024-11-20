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

package org.apache.inlong.manager.schedule.airflow.util;

import org.apache.inlong.manager.schedule.ScheduleUnit;

import java.math.BigInteger;
import java.util.Objects;

public class DateUtil {

    public static String intervalToSeconds(long interval, String timeUnit) {
        BigInteger seconds = new BigInteger(String.valueOf(interval));
        String intervalStr = "";
        switch (Objects.requireNonNull(ScheduleUnit.getScheduleUnit(timeUnit))) {
            case SECOND:
                intervalStr = "1";
                break;
            case MINUTE:
                intervalStr = "60";
                break;
            case HOUR:
                intervalStr = "3600";
                break;
            case DAY:
                intervalStr = "86400";
                break;
            case WEEK:
                intervalStr = "604800";
                break;
            case MONTH:
                intervalStr = "2592000";
                break;
            case YEAR:
                intervalStr = "31536000";
                break;
            default:
                throw new IllegalArgumentException("Unsupported time unit");
        }
        return seconds.multiply(new BigInteger(intervalStr)).toString();
    }

}
