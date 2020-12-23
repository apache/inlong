/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.conf;

import org.apache.tubemq.agent.constants.JobConstants;

/**
 * profile used in trigger. Trigger profile is a special job profile with
 * trigger config.
 */
public class TriggerProfile extends JobProfile {

    @Override
    public boolean allRequiredKeyExist() {
        return hasKey(JobConstants.JOB_TRIGGER) && hasKey(JobConstants.JOB_ID);
    }

    public static TriggerProfile parseJsonStr(String jsonStr) {
        TriggerProfile conf = new TriggerProfile();
        conf.loadJsonStrResource(jsonStr);
        return conf;
    }
}
