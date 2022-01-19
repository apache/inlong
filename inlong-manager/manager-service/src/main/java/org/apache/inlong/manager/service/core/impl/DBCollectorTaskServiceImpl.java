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

package org.apache.inlong.manager.service.core.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.inlong.manager.common.enums.DBCollectorDetailTaskEntityStatus;
import org.apache.inlong.manager.common.enums.DBCollectorTaskConstant;
import org.apache.inlong.manager.common.enums.DBCollectorTaskReturnCode;
import org.apache.inlong.manager.common.pojo.dbcollector.DBCollectorReportTaskRequest;
import org.apache.inlong.manager.common.pojo.dbcollector.DBCollectorTaskInfo;
import org.apache.inlong.manager.common.pojo.dbcollector.DBCollectorTaskRequest;
import org.apache.inlong.manager.dao.entity.DBCollectorDetailTaskEntity;
import org.apache.inlong.manager.dao.mapper.DBCollectorDetailTaskMapper;
import org.apache.inlong.manager.service.core.DBCollectorTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DBCollectorTaskServiceImpl implements DBCollectorTaskService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBCollectorTaskServiceImpl.class);

    @Autowired
    private DBCollectorDetailTaskMapper detailTaskMapper;

    @Override
    public DBCollectorTaskInfo getTask(DBCollectorTaskRequest req) {
        LOGGER.warn("DBCollectorTaskInfo ", req.toString());
        if (!DBCollectorTaskConstant.INTERFACE_VERSION.equals(req.getVersion())) {
            DBCollectorTaskInfo taskInfo = DBCollectorTaskInfo.builder()
                    .version(DBCollectorTaskConstant.INTERFACE_VERSION)
                    .returnCode(DBCollectorTaskReturnCode.INVALID_VERSION.getCode()).build();
            return taskInfo;
        }
        DBCollectorDetailTaskEntity entity = detailTaskMapper
                .selectOneByState(DBCollectorDetailTaskEntityStatus.INIT.getCode());
        if (entity == null) {
            DBCollectorTaskInfo taskInfo = DBCollectorTaskInfo.builder()
                    .version(DBCollectorTaskConstant.INTERFACE_VERSION)
                    .returnCode(DBCollectorTaskReturnCode.EMPTY.getCode()).build();
            return taskInfo;
        }
        int ret = detailTaskMapper.changeState(entity.getId(), 0, DBCollectorDetailTaskEntityStatus.INIT.getCode(),
                DBCollectorDetailTaskEntityStatus.DISPATCHED.getCode());
        if (ret == 0) {
            DBCollectorTaskInfo taskInfo = DBCollectorTaskInfo.builder()
                    .version(DBCollectorTaskConstant.INTERFACE_VERSION)
                    .returnCode(DBCollectorTaskReturnCode.EMPTY.getCode()).build();
            return taskInfo;
        } else {
            DBCollectorTaskInfo.TaskInfo task = new DBCollectorTaskInfo.TaskInfo();
            task.setId(entity.getId());
            task.setType(entity.getType());
            task.setDBType(entity.getDbType());
            task.setIp(entity.getIp());
            task.setDBPort(entity.getPort());
            task.setDBName(entity.getDbName());
            task.setUser(entity.getUser());
            task.setPassword(entity.getPassword());
            task.setSqlStatement(entity.getSqlStatement());
            task.setTotalLimit(entity.getTotalLimit());
            task.setOnceLimit(entity.getOnceLimit());
            task.setTimeLimit(entity.getTimeLimit());
            task.setRetryTimes(entity.getRetryTimes());
            task.setInlongGroupId(entity.getGroupId());
            task.setInlongStreamId(entity.getStreamId());
            DBCollectorTaskInfo taskInfo = DBCollectorTaskInfo.builder()
                    .version(DBCollectorTaskConstant.INTERFACE_VERSION)
                    .returnCode(DBCollectorTaskReturnCode.SUCC.getCode())
                    .data(task).build();
            return taskInfo;
        }
    }

    @Override
    public Integer reportTask(DBCollectorReportTaskRequest req) {
        if (req.getVersion() != DBCollectorTaskConstant.INTERFACE_VERSION) {
            return DBCollectorTaskReturnCode.INVALID_VERSION.getCode();
        }
        DBCollectorDetailTaskEntity entity = detailTaskMapper
                .selectByTaskId(req.getId());
        if (entity == null) {
            return DBCollectorTaskReturnCode.EMPTY.getCode();
        }
        if (req.getState() != DBCollectorDetailTaskEntityStatus.DISPATCHED.getCode()
                && req.getState() != DBCollectorDetailTaskEntityStatus.DONE.getCode()
                && req.getState() != DBCollectorDetailTaskEntityStatus.FAILED.getCode()) {
            return DBCollectorTaskReturnCode.INVALID_STATE.getCode();
        }
        int ret = detailTaskMapper
                .changeState(entity.getId(), req.getOffset(), DBCollectorDetailTaskEntityStatus.DISPATCHED.getCode(),
                        req.getState());
        if (ret == 0) {
            return DBCollectorTaskReturnCode.EMPTY.getCode();
        }
        return DBCollectorTaskReturnCode.SUCC.getCode();
    }
}
