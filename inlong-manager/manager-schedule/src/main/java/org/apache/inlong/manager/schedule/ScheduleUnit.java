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

package org.apache.inlong.manager.schedule;

import lombok.Getter;

@Getter
public enum ScheduleUnit {

    YEAR("Y"),
    MONTH("M"),
    DAY("D"),
    WEEK("W"),
    HOUR("H"),
    MINUTE("I"),
    SECOND("S"),
    ONE_ROUND("O");

    final String unit;

    ScheduleUnit(String unit) {
        this.unit = unit;
    }

    public static ScheduleUnit getScheduleUnit(String unit) {
        for (ScheduleUnit scheduleUnit : ScheduleUnit.values()) {
            if (scheduleUnit.unit.equalsIgnoreCase(unit)) {
                return scheduleUnit;
            }
        }
        return null;
    }
}
