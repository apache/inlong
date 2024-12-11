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

package org.apache.inlong.manager.service.dirtyData.impl;

import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.DirtyQueryLogEntity;
import org.apache.inlong.manager.dao.mapper.DirtyQueryLogEntityMapper;
import org.apache.inlong.manager.pojo.sink.DirtyDataDetailResponse;
import org.apache.inlong.manager.pojo.sink.DirtyDataRequest;
import org.apache.inlong.manager.pojo.sink.DirtyDataResponse;
import org.apache.inlong.manager.pojo.sink.DirtyDataTrendDetailResponse;
import org.apache.inlong.manager.pojo.sink.DirtyDataTrendRequest;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.service.dirtyData.DirtyQueryLogService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class DirtyQueryLogServiceImpl implements DirtyQueryLogService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DirtyQueryLogServiceImpl.class);

    @Autowired
    private DirtyQueryLogEntityMapper dirtyQueryLogEntityMapper;
    @Autowired
    private ObjectMapper objectMapper;

    @Value("${dirty.log.clean.enabled:false}")
    private Boolean dirtyLogCleanEnabled;
    @Value("${dirty.log.clean.interval.minutes:5}")
    private Integer dirtyLogCleanInterval;
    @Value("${dirty.log.retention.minutes:10}")
    private Integer retentionMinutes;
    @Value("${dirty.log.db.table:inlong_iceberg::dirty_data_achive_iceberg}")
    private String dirtyDataDbTable;

    @PostConstruct
    private void startDirtyLogCleanTask() {
        if (dirtyLogCleanEnabled) {
            ThreadFactory factory = new ThreadFactoryBuilder()
                    .setNameFormat("scheduled-dirtyQueryLog-deleted-%d")
                    .setDaemon(true)
                    .build();
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(factory);
            executor.scheduleWithFixedDelay(() -> {
                try {
                    LOGGER.info("begin to clean dirty query log");
                    dirtyQueryLogEntityMapper.updateToTimeout(retentionMinutes);
                    LOGGER.info("success to clean dirty query log successfully");
                } catch (Throwable t) {
                    LOGGER.error("clean dirty query log error", t);
                }
            }, 0, dirtyLogCleanInterval, TimeUnit.MINUTES);
            LOGGER.info("clean dirty query log started successfully");
        }
    }

    @Override
    public DirtyDataResponse listDirtyDataTrend(DirtyDataTrendRequest request) {
        if (CollectionUtils.isEmpty(request.getSinkIdList())) {
            return null;
        }
        try {
            DirtyDataResponse dirtyDataResponse = new DirtyDataResponse();
            String requestStr = objectMapper.writeValueAsString(request);
            String md5 = DigestUtils.md5Hex(requestStr);
            DirtyQueryLogEntity dirtyQueryLogEntity = dirtyQueryLogEntityMapper.selectByMd5(md5);
            if (dirtyQueryLogEntity != null) {
                LOGGER.info("dirty query log is exist");
                dirtyDataResponse.setTaskId(dirtyQueryLogEntity.getTaskId());
                return dirtyDataResponse;
            }
            DirtyQueryLogEntity dirtyQueryLog = new DirtyQueryLogEntity();
            // TODO dirtyQueryLog.setTaskId();
            dirtyQueryLog.setMd5(md5);
            dirtyQueryLog.setRequestParams(requestStr);
            dirtyQueryLog.setCreator(LoginUserUtils.getLoginUser().getName());
            dirtyQueryLog.setModifier(LoginUserUtils.getLoginUser().getName());
            dirtyQueryLogEntityMapper.insert(dirtyQueryLog);

            return dirtyDataResponse;
        } catch (Exception e) {
            throw new BusinessException("list dirty data trend failed");
        }
    }

    @Override
    public DirtyDataResponse listDirtyData(DirtyDataRequest request) {
        if (CollectionUtils.isEmpty(request.getSinkIdList())) {
            return null;
        }
        if (request.getDataCount() == null) {
            request.setDataCount(10);
        }
        try {
            DirtyDataResponse dirtyDataResponse = new DirtyDataResponse();
            String requestStr = objectMapper.writeValueAsString(request);
            String md5 = DigestUtils.md5Hex(requestStr);
            DirtyQueryLogEntity dirtyQueryLogEntity = dirtyQueryLogEntityMapper.selectByMd5(md5);
            if (dirtyQueryLogEntity != null) {
                LOGGER.info("dirty query log is exist");
                dirtyDataResponse.setTaskId(dirtyQueryLogEntity.getTaskId());
                return dirtyDataResponse;
            }
            DirtyQueryLogEntity dirtyQueryLog = new DirtyQueryLogEntity();
            // TODO dirtyQueryLog.setTaskId();
            dirtyQueryLog.setMd5(md5);
            dirtyQueryLog.setRequestParams(requestStr);
            dirtyQueryLog.setCreator(LoginUserUtils.getLoginUser().getName());
            dirtyQueryLog.setModifier(LoginUserUtils.getLoginUser().getName());
            dirtyQueryLogEntityMapper.insert(dirtyQueryLog);
            return dirtyDataResponse;
        } catch (Exception e) {
            LOGGER.error("list dirty data failed", e);
            throw new BusinessException("list dirty data failed");
        }
    }

    @Override
    public List<DirtyDataDetailResponse> getDirtyData(String taskId) {
        try {
            // TODO
            return new ArrayList<>();
        } catch (Exception e) {
            LOGGER.error("get dirty data failed", e);
            throw new BusinessException("get dirty data failed");
        }
    }

    @Override
    public List<DirtyDataTrendDetailResponse> getDirtyDataTrend(String taskId) {
        try {
            // TODO
            return new ArrayList<>();
        } catch (Exception e) {
            LOGGER.error("get dirty data trend failed", e);
            throw new BusinessException("get dirty data trend failed");
        }
    }

    @Override
    public String getSqlTaskStatus(String taskId) {
        try {
            // TODO
            return "success";
        } catch (Exception e) {
            LOGGER.error("get sql task status failed", e);
            throw new BusinessException("get get sql task status failed");
        }
    }
}
