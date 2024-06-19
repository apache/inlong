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
 * Build-in schedule engine, provides basic schedule capabilities.
 * */
public interface ScheduleEngine {

    /**
     * Start schedule engine.
     * */
    void start();

    /**
     * Handle schedule register.
     * @param scheduleInfo schedule info to register
     * */
    boolean handleRegister(ScheduleInfo scheduleInfo);

    /**
     * Handle schedule unregister.
     * @param groupId group to un-register schedule info
     * */
    boolean handleUnregister(String groupId);

    /**
     * Handle schedule update.
     * @param scheduleInfo schedule info to update
     * */
    boolean handleUpdate(ScheduleInfo scheduleInfo);

    /**
     * Stop schedule engine.
     * */
    void stop();
}
