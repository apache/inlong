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
import org.apache.inlong.manager.common.pojo.agent.FileAgentTaskConfig;
import org.apache.inlong.manager.common.pojo.datasource.SourceFileDetailPageRequest;
import org.apache.inlong.manager.dao.entity.SourceFileDetailEntity;
import org.springframework.stereotype.Repository;

@Repository
public interface SourceFileDetailEntityMapper {

    int deleteByPrimaryKey(Integer id);

    int insert(SourceFileDetailEntity record);

    int insertSelective(SourceFileDetailEntity record);

    SourceFileDetailEntity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(SourceFileDetailEntity record);

    int updateByPrimaryKey(SourceFileDetailEntity record);

    List<SourceFileDetailEntity> selectByCondition(SourceFileDetailPageRequest request);

    /**
     * Query whether the same file data source details exist
     *
     * @param bid business identifier
     * @param dsid data stream identifier
     * @param ip IP of file source
     * @param username user name corresponding to the data source IP
     * @return number of eligible file sources
     */
    Integer selectDetailExist(@Param("bid") String bid, @Param("dsid") String dsid, @Param("ip") String ip,
            @Param("username") String username);

    List<FileAgentTaskConfig> selectFileAgentTaskByIp(@Param("ip") String agentIp);

    List<FileAgentTaskConfig> selectFileAgentTaskByIpForCheck(@Param("ip") String agentIp);

    /**
     * According to business identifier and data source identifier, query file source details
     *
     * @param bid business identifier
     * @param dsid data stream identifier
     * @return file source list
     */
    List<SourceFileDetailEntity> selectByIdentifier(@Param("bid") String bid, @Param("dsid") String dsid);

    /**
     * According to business identifier and data stream identifier, physically delete file data source details
     *
     * @return rows deleted
     */
    int deleteByIdentifier(@Param("bid") String bid, @Param("dsid") String dsid);

    /**
     * According to business identifier and data stream identifier, logically delete file data source details
     *
     * @return rows updated
     */
    int logicDeleteByIdentifier(@Param("bid") String bid, @Param("dsid") String dsid,
            @Param("operator") String operator);

    List<SourceFileDetailEntity> selectByIp(@Param("ip") String ip);

}