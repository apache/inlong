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

package org.apache.inlong.agent.conf;

import org.apache.inlong.agent.constants.JobConstants;

/**
 * profile used in trigger. Trigger profile is a special job profile
 */
public class TriggerProfile extends JobProfile {

    @Override
    public boolean allRequiredKeyExist() {
        return hasKey(JobConstants.JOB_TRIGGER) && super.allRequiredKeyExist();
    }

    public static TriggerProfile parseJsonStr(String jsonStr) {
        TriggerProfile conf = new TriggerProfile();
        conf.loadJsonStrResource(jsonStr);
        return conf;
    }

    public String getTriggerId() {
        return get(JobConstants.JOB_ID);
    }

    public static TriggerProfile parseJobProfile(JobProfile jobProfile) {
        TriggerProfile conf = new TriggerProfile();
        conf.loadJsonStrResource(jobProfile.toJsonStr());
        return conf;
    }

    public Integer getOpType() {
        return getInt(JobConstants.JOB_OP);
    }

    public String getDeliveryTime() {
        return get(JobConstants.JOB_DELIVERY_TIME);
    }
}
