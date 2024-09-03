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

import org.apache.inlong.manager.pojo.schedule.ScheduleInfo;

/**
 * Interface for schedule engine client which responses for communicating with schedule engine.
 * */
public interface ScheduleEngineClient {

    /**
     * Check whether scheduleEngine type is matched.
     * */
    boolean accept(String engineType);

    /**
     * Register schedule to schedule engine.
     * @param scheduleInfo schedule info to register
     * */
    boolean register(ScheduleInfo scheduleInfo);

    /**
     * Un-register schedule from schedule engine.
     *
     * @param groupId schedule info to unregister
     */
    boolean unregister(String groupId);

    /**
     * Update schedule from schedule engine.
     * @param scheduleInfo schedule info to update
     * */
    boolean update(ScheduleInfo scheduleInfo);

}
