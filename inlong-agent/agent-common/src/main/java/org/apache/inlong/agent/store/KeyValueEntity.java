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

package org.apache.inlong.agent.store;

import org.apache.inlong.agent.conf.InstanceProfile;
import org.apache.inlong.agent.conf.JobProfile;
import org.apache.inlong.agent.conf.OffsetProfile;
import org.apache.inlong.agent.conf.TaskProfile;

/**
 * key value entity. key is string and value is a json
 */
public class KeyValueEntity {

    private String key;

    private StateSearchKey stateSearchKey;

    /**
     * stores the file name that the jsonValue refers
     */
    private String fileName;

    private String jsonValue;

    private KeyValueEntity() {

    }

    public KeyValueEntity(String key, String jsonValue, String fileName) {
        this.key = key;
        this.jsonValue = jsonValue;
        this.stateSearchKey = StateSearchKey.ACCEPTED;
        this.fileName = fileName;
    }

    public String getKey() {
        return key;
    }

    public String getFileName() {
        return fileName;
    }

    public StateSearchKey getStateSearchKey() {
        return stateSearchKey;
    }

    public KeyValueEntity setStateSearchKey(StateSearchKey stateSearchKey) {
        this.stateSearchKey = stateSearchKey;
        return this;
    }

    public String getJsonValue() {
        return jsonValue;
    }

    public KeyValueEntity setJsonValue(String jsonValue) {
        this.jsonValue = jsonValue;
        return this;
    }

    /**
     * convert keyValue to job profile
     *
     * @return JobConfiguration
     */
    public JobProfile getAsJobProfile() {
        // convert jsonValue to jobConfiguration
        return JobProfile.parseJsonStr(getJsonValue());
    }

    /**
     * convert keyValue to offset profile
     */
    public OffsetProfile getAsOffsetProfile() {
        return OffsetProfile.parseJsonStr(getJsonValue());
    }

    /**
     * convert keyValue to task profile
     */
    public TaskProfile getAsTaskProfile() {
        return TaskProfile.parseJsonStr(getJsonValue());
    }

    /**
     * convert keyValue to instance profile
     */
    public InstanceProfile getAsInstanceProfile() {
        return InstanceProfile.parseJsonStr(getJsonValue());
    }

    /**
     * check whether the entity is finished
     */
    public boolean checkFinished() {
        return stateSearchKey.equals(StateSearchKey.SUCCESS)
                || stateSearchKey.equals(StateSearchKey.FAILED);
    }
}
