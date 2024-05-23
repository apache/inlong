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

package org.apache.inlong.manager.dao.mapper;

import org.apache.inlong.manager.dao.DaoBaseTest;
import org.apache.inlong.manager.dao.entity.ScheduleEntity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Timestamp;
import java.util.Date;

public class ScheduleEntityTest extends DaoBaseTest {

    public static final String GROUP_ID_PREFIX = "test_group_";
    public static final String USER = "admin";
    public static final int SCHEDULE_TYPE = 0;
    public static final int SCHEDULE_TYPE_NEW = 1;
    public static final String SCHEDULE_UNIT = "H";
    public static final String SCHEDULE_UNIT_NEW = "D";
    public static final int SCHEDULE_INTERVAL = 1;
    public static final int SCHEDULE_INTERVAL_NEW = 1;
    public static final Timestamp DEFAULT_TIME = new Timestamp(System.currentTimeMillis());

    @Autowired
    ScheduleEntityMapper scheduleEntityMapper;

    @Test
    public void testSelectByGroupId() throws Exception {
        ScheduleEntity scheduleEntity = genEntity();
        scheduleEntityMapper.insert(scheduleEntity);
        ScheduleEntity entityQueried = scheduleEntityMapper.selectByGroupId(scheduleEntity.getInlongGroupId());
        Assertions.assertEquals(scheduleEntity.getInlongGroupId(), entityQueried.getInlongGroupId());
        Assertions.assertEquals(SCHEDULE_TYPE, entityQueried.getScheduleType());
        Assertions.assertEquals(SCHEDULE_UNIT, entityQueried.getScheduleUnit());
        Assertions.assertEquals(SCHEDULE_INTERVAL, entityQueried.getScheduleInterval());
        Assertions.assertEquals(DEFAULT_TIME, entityQueried.getStartTime());
        Assertions.assertEquals(DEFAULT_TIME, entityQueried.getEndTime());
        Assertions.assertEquals(USER, entityQueried.getCreator());
    }

    @Test
    public void testUpdate() throws Exception {
        ScheduleEntity scheduleEntity = genEntity();
        scheduleEntityMapper.insert(scheduleEntity);
        ScheduleEntity entityQueried = scheduleEntityMapper.selectByGroupId(scheduleEntity.getInlongGroupId());
        Assertions.assertEquals(scheduleEntity.getInlongGroupId(), entityQueried.getInlongGroupId());
        Assertions.assertEquals(SCHEDULE_TYPE, entityQueried.getScheduleType());
        Assertions.assertEquals(SCHEDULE_UNIT, entityQueried.getScheduleUnit());
        Assertions.assertEquals(SCHEDULE_INTERVAL, entityQueried.getScheduleInterval());
        Assertions.assertEquals(DEFAULT_TIME, entityQueried.getStartTime());
        Assertions.assertEquals(DEFAULT_TIME, entityQueried.getEndTime());
        Assertions.assertEquals(USER, entityQueried.getCreator());

        entityQueried.setScheduleType(SCHEDULE_TYPE_NEW);
        entityQueried.setScheduleUnit(SCHEDULE_UNIT_NEW);
        entityQueried.setScheduleInterval(SCHEDULE_INTERVAL_NEW);
        scheduleEntityMapper.updateByIdSelective(entityQueried);
        entityQueried = scheduleEntityMapper.selectByGroupId(scheduleEntity.getInlongGroupId());
        Assertions.assertEquals(scheduleEntity.getInlongGroupId(), entityQueried.getInlongGroupId());
        Assertions.assertEquals(SCHEDULE_TYPE_NEW, entityQueried.getScheduleType());
        Assertions.assertEquals(SCHEDULE_UNIT_NEW, entityQueried.getScheduleUnit());
        Assertions.assertEquals(SCHEDULE_INTERVAL_NEW, entityQueried.getScheduleInterval());
        Assertions.assertEquals(DEFAULT_TIME, entityQueried.getStartTime());
        Assertions.assertEquals(DEFAULT_TIME, entityQueried.getEndTime());
        Assertions.assertEquals(USER, entityQueried.getCreator());
    }

    @Test
    public void testDelete() throws Exception {
        ScheduleEntity scheduleEntity = genEntity();
        scheduleEntityMapper.insert(scheduleEntity);
        ScheduleEntity entityQueried = scheduleEntityMapper.selectByGroupId(scheduleEntity.getInlongGroupId());
        Assertions.assertEquals(scheduleEntity.getInlongGroupId(), entityQueried.getInlongGroupId());
        Assertions.assertEquals(SCHEDULE_TYPE, entityQueried.getScheduleType());
        Assertions.assertEquals(SCHEDULE_UNIT, entityQueried.getScheduleUnit());
        Assertions.assertEquals(SCHEDULE_INTERVAL, entityQueried.getScheduleInterval());
        Assertions.assertEquals(DEFAULT_TIME, entityQueried.getStartTime());
        Assertions.assertEquals(DEFAULT_TIME, entityQueried.getEndTime());
        Assertions.assertEquals(USER, entityQueried.getCreator());

        scheduleEntityMapper.deleteByGroupId(scheduleEntity.getInlongGroupId());
        entityQueried = scheduleEntityMapper.selectByGroupId(scheduleEntity.getInlongGroupId());
        Assertions.assertNull(entityQueried);
    }

    private ScheduleEntity genEntity() {
        ScheduleEntity entity = new ScheduleEntity();
        entity.setInlongGroupId(GROUP_ID_PREFIX + System.currentTimeMillis());
        entity.setScheduleType(SCHEDULE_TYPE);
        entity.setScheduleUnit(SCHEDULE_UNIT);
        entity.setScheduleInterval(SCHEDULE_INTERVAL);
        entity.setStartTime(DEFAULT_TIME);
        entity.setEndTime(DEFAULT_TIME);
        entity.setCreator(USER);
        entity.setCreateTime(new Date());
        entity.setModifyTime(new Date());
        entity.setIsDeleted(0);
        return entity;
    }
}
