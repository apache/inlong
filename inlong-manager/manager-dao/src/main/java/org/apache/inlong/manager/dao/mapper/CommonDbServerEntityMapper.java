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

import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.apache.inlong.manager.common.pojo.commonserver.CommonDbServerPageRequest;
import org.apache.inlong.manager.dao.entity.CommonDbServerEntity;
import org.springframework.stereotype.Repository;

@Repository
public interface CommonDbServerEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(CommonDbServerEntity record);

    int insertSelective(CommonDbServerEntity record);

    CommonDbServerEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(CommonDbServerEntity record);

    int updateByPrimaryKey(CommonDbServerEntity record);

    List<CommonDbServerEntity> selectByUsernameAndIpPort(@Param("username") String username,
                                                         @Param("dbType") String dbType,
                                                         @Param("dbServerIp") String dbServerIp,
                                                         @Param("port") int port);

    List<CommonDbServerEntity> selectAll();

    List<CommonDbServerEntity> selectByCondition(CommonDbServerPageRequest request);
}