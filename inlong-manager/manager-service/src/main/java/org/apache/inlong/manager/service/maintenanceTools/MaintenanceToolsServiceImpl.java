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

package org.apache.inlong.manager.service.maintenanceTools;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSinkEntity;
import org.apache.inlong.manager.dao.mapper.InlongStreamEntityMapper;
import org.apache.inlong.manager.dao.mapper.StreamSinkEntityMapper;
import org.apache.inlong.manager.pojo.consume.SortConsumerInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.group.pulsar.InlongPulsarInfo;
import org.apache.inlong.manager.pojo.user.LoginUserUtils;
import org.apache.inlong.manager.pojo.user.UserRoleCode;
import org.apache.inlong.manager.service.group.InlongGroupService;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperator;
import org.apache.inlong.manager.service.resource.queue.QueueResourceOperatorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Service
public class MaintenanceToolsServiceImpl implements MaintenanceToolsService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaintenanceToolsServiceImpl.class);

    @Autowired
    private InlongGroupService groupService;
    @Autowired
    private StreamSinkEntityMapper sinkEntityMapper;
    @Autowired
    private InlongStreamEntityMapper streamEntityMapper;
    @Autowired
    private QueueResourceOperatorFactory queueOperatorFactory;

    @Override
    public List<SortConsumerInfo> getSortConsumer(MultipartFile file) {
        LoginUserUtils.getLoginUser().getRoles().add(UserRoleCode.INLONG_SERVICE);
        List<SortConsumerInfo> sortConsumerInfoList = new ArrayList<>();
        try (BufferedReader bufferedReader =
                new BufferedReader(new InputStreamReader((file.getInputStream()), StandardCharsets.UTF_8))) {
            String readerStr = null;
            while ((readerStr = bufferedReader.readLine()) != null) {
                String[] sinkIdList = readerStr.split(InlongConstants.COMMA);
                for (String sinkIdStr : sinkIdList) {
                    Integer sinkId = Integer.valueOf(sinkIdStr);
                    StreamSinkEntity sinkEntity = sinkEntityMapper.selectByPrimaryKey(sinkId);

                    InlongGroupInfo groupInfo = groupService.get(sinkEntity.getInlongGroupId());
                    InlongStreamEntity streamEntity = streamEntityMapper
                            .selectByIdentifier(sinkEntity.getInlongGroupId(), sinkEntity.getInlongStreamId());
                    QueueResourceOperator queueOperator = queueOperatorFactory.getInstance(groupInfo.getMqType());

                    String consumerGroup = queueOperator.getSortConsumeGroup(groupInfo, streamEntity, sinkEntity);

                    SortConsumerInfo sortConsumerInfo = SortConsumerInfo.builder()
                            .sinkId(sinkId)
                            .consumerGroup(consumerGroup)
                            .inlongGroupId(sinkEntity.getInlongGroupId())
                            .inlongStreamId(sinkEntity.getInlongStreamId())
                            .build();
                    sortConsumerInfoList.add(sortConsumerInfo);
                }
            }
            LOGGER.info("success get sort consumer");
            return sortConsumerInfoList;
        } catch (IOException e) {
            LOGGER.error("get sort consumer failed:", e);
            throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Can not properly read update file");
        } finally {
            LoginUserUtils.getLoginUser().getRoles().remove(UserRoleCode.INLONG_SERVICE);
        }
    }

    @Override
    public Boolean resetCursor(MultipartFile file, String resetTime) {
        LoginUserUtils.getLoginUser().getRoles().add(UserRoleCode.INLONG_SERVICE);
        try (BufferedReader bufferedReader =
                new BufferedReader(new InputStreamReader((file.getInputStream()), StandardCharsets.UTF_8))) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = sdf.parse(resetTime);
            long timeStamp = date.getTime();
            String readerStr = null;
            while ((readerStr = bufferedReader.readLine()) != null) {
                String[] sinkIdList = readerStr.split(InlongConstants.COMMA);
                for (String sinkIdStr : sinkIdList) {
                    Integer sinkId = Integer.valueOf(sinkIdStr);
                    StreamSinkEntity sinkEntity = sinkEntityMapper.selectByPrimaryKey(sinkId);
                    InlongGroupInfo groupInfo = groupService.get(sinkEntity.getInlongGroupId());
                    InlongPulsarInfo pulsarInfo = (InlongPulsarInfo) groupInfo;
                    InlongStreamEntity streamEntity = streamEntityMapper
                            .selectByIdentifier(sinkEntity.getInlongGroupId(), sinkEntity.getInlongStreamId());
                    QueueResourceOperator queueOperator = queueOperatorFactory.getInstance(groupInfo.getMqType());
                    queueOperator.resetCursor(groupInfo, streamEntity, sinkEntity, timeStamp);
                }
            }
            LOGGER.info("success reset cursor consumer");
        } catch (Exception e) {
            LOGGER.error("reset cursor consumer failed:", e);
            throw new BusinessException(ErrorCodeEnum.INVALID_PARAMETER, "Can not properly read update file");
        }
        return true;
    }

}
