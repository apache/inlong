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

package org.apache.inlong.agent.db;

import java.util.List;
import org.apache.inlong.agent.conf.TriggerProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommandDb {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandDb.class);
    public static final int MANAGER_SUCCESS_CODE = 0;
    public static final int MANAGER_FAIL_CODE = 1;
    private final Db db;

    public CommandDb(Db db) {
        this.db = db;
    }

    /**
     * store manager command to db
     * @param commandEntity
     */
    public void storeCommand(CommandEntity commandEntity) {
        db.putCommand(commandEntity);
    }

    /**
     * get those commands not ack to manager
     * @return
     */
    public List<CommandEntity> getUnackedCommands() {
        return db.searchCommands(false);
    }


    /**
     * save normal command result for trigger
     * @param profile
     * @param success
     */
    public void saveNormalCmds(TriggerProfile profile, boolean success) {
        CommandEntity entity = new CommandEntity();
        entity.setId(CommandEntity.generateCommanid(profile.getTriggerId(), profile.getOpType()));
        entity.setTaskId(profile.getTriggerId());
        entity.setDeliveryTime(profile.getDeliveryTime());
        entity.setCommandResult(success ? MANAGER_SUCCESS_CODE : MANAGER_FAIL_CODE);
        entity.setAcked(false);
        storeCommand(entity);
    }

    /**
     * save special command result for trigger (retry\makeup\check)
     * @param id
     * @param taskId
     * @param success
     */
    public void saveSpecialCmds(Integer id, Integer taskId, boolean success) {
        CommandEntity entity = new CommandEntity();
        entity.setId(String.valueOf(id));
        entity.setTaskId(String.valueOf(taskId));
        entity.setAcked(false);
        entity.setCommandResult(success ? MANAGER_SUCCESS_CODE : MANAGER_FAIL_CODE);
        storeCommand(entity);
    }
}
