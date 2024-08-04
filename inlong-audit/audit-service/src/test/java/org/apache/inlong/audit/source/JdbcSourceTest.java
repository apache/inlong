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

package org.apache.inlong.audit.source;

import org.apache.inlong.audit.entities.StartEndTime;

import org.junit.Test;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JdbcSourceTest {

    @Test
    public void getStatCycleOfMinute() {
        JdbcSource jdbcSource = new JdbcSource(null, null);

        List<StartEndTime> startEndTime = jdbcSource.getStatCycleOfMinute(1, 5);
        assertEquals(12, startEndTime.size());

        DateTimeFormatter formatterHour = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalTime firstElement = LocalTime.parse(startEndTime.get(0).getStartTime(), formatterHour);
        LocalTime lastElement = LocalTime.parse(startEndTime.get(11).getEndTime(), formatterHour);
        Duration duration = Duration.between(firstElement, lastElement);
        assertEquals(60, duration.toMinutes());

        DateTimeFormatter formatterMinutes = DateTimeFormatter.ofPattern("HH:mm:ss");
        for (StartEndTime entry : startEndTime) {
            LocalTime startTime = LocalTime.parse(entry.getStartTime().substring(11), formatterMinutes);
            boolean check = startTime.getMinute() % 5 == 0 && startTime.getSecond() == 0;
            assertTrue(check);

            LocalTime endTime = LocalTime.parse(entry.getEndTime().substring(11), formatterMinutes);
            check = endTime.getMinute() % 5 == 0 && endTime.getSecond() == 0;
            assertTrue(check);
        }
    }
}
