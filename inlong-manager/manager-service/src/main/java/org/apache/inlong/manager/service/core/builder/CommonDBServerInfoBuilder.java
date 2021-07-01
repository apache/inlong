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

package org.apache.inlong.manager.service.core.builder;

import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerInfo;
import org.apache.inlong.manager.dao.entity.CommonDbServerEntity;

public class CommonDBServerInfoBuilder {

    public static CommonDbServerInfo buildInfoFromEntity(CommonDbServerEntity entity) {
        return CommonDbServerInfo.builder()
                .id(entity.getId())
                .connectionName(entity.getConnectionName())
                .dbType(entity.getDbType())
                .dbServerIp(entity.getDbServerIp())
                .port(entity.getPort())
                .dbName(entity.getDbName())
                .username(entity.getUsername())
                .password(entity.getPassword())
                .hasSelect(entity.getHasSelect())
                .hasInsert(entity.getHasInsert())
                .hasUpdate(entity.getHasUpdate())
                .hasDelete(entity.getHasDelete())
                .inCharges(entity.getInCharges())
                .dbDescription(entity.getDbDescription())
                .backupDbServerIp(entity.getBackupDbServerIp())
                .backupDbPort(entity.getBackupDbPort())
                .creator(entity.getCreator())
                .modifier(entity.getModifier())
                .createTime(entity.getCreateTime())
                .modifyTime(entity.getModifyTime())
                .visiblePerson(entity.getVisiblePerson())
                .visibleGroup(entity.getVisibleGroup())
                .isDeleted(entity.getIsDeleted())
                .accessType(entity.getAccessType())
                .status(entity.getStatus())
                .build();
    }

}
