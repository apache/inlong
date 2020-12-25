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
package org.apache.tubemq.agent.db;


import java.util.ArrayList;
import java.util.List;
import org.apache.tubemq.agent.conf.TriggerProfile;
import org.apache.tubemq.agent.constants.JobConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * db interface for trigger profile.
 */
public class TriggerProfileDB {

    private static final Logger LOGGER = LoggerFactory.getLogger(TriggerProfileDB.class);

    // prefix for trigger profile.
    private static final String TRIGGER_ID_PREFIX = "trigger_";

    private final DB db;

    public TriggerProfileDB(DB db) {
        this.db = db;
    }

    /**
     * get trigger list from db.
     * @return - list of trigger
     */
    public List<TriggerProfile> getTriggers() {
        // potential performance issue, needs to find out the speed.
        List<KeyValueEntity> result = this.db.findAll(TRIGGER_ID_PREFIX);
        List<TriggerProfile> triggerList = new ArrayList<>();
        for (KeyValueEntity entity : result) {
            triggerList.add(entity.getAsTriggerProfile());
        }
        return triggerList;
    }

    /**
     * store trigger profile.
     * @param trigger - trigger
     */
    public void storeTrigger(TriggerProfile trigger) {
        if (trigger.allRequiredKeyExist()) {
            String keyName = TRIGGER_ID_PREFIX + trigger.get(JobConstants.JOB_ID);
            KeyValueEntity entity = new KeyValueEntity(keyName, trigger.toJsonStr());
            KeyValueEntity oldEntity = db.put(entity);
            if (oldEntity != null) {
                LOGGER.warn("trigger profile {} has been replaced", oldEntity.getKey());
            }
        }
    }

    /**
     * delete trigger by id.
     * @param id
     */
    public void deleteTrigger(String id) {
        String triggerKey = TRIGGER_ID_PREFIX + id;
        db.remove(triggerKey);
    }
}
