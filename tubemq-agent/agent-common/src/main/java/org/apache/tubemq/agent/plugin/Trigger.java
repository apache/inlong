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
package org.apache.tubemq.agent.plugin;

import java.io.IOException;
import org.apache.tubemq.agent.conf.JobProfile;
import org.apache.tubemq.agent.conf.TriggerProfile;

/**
 * Trigger interface, which generates job in condition.
 */
public interface Trigger {

    /**
     * init trigger by trigger profile
     * @param profile - trigger profile.
     */
    void init(TriggerProfile profile) throws IOException;

    /**
     * run trigger.
     */
    void run();

    /**
     * destroy trigger.
     */
    void destroy();

    /**
     * fetch job profile from trigger
     * @return - job profile
     */
    JobProfile fetchJobProfile();
}
