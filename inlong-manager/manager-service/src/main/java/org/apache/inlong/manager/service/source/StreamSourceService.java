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

package org.apache.inlong.manager.service.source;

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.common.pojo.source.SourceSnapshotRequest;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.source.SourcePageRequest;
import org.apache.inlong.manager.common.pojo.source.SourceRequest;
import org.apache.inlong.manager.common.pojo.source.SourceResponse;

import java.util.List;

/**
 * Service layer interface for stream source
 */
public interface StreamSourceService {

    /**
     * Save the source information
     *
     * @param request Source request.
     * @param operator Operator's name.
     * @return source id after saving.
     */
    Integer save(SourceRequest request, String operator);

    /**
     * Query source information based on id and type.
     *
     * @param id source id.
     * @param sourceType Source type.
     * @return Source info
     */
    SourceResponse get(Integer id, String sourceType);

    /**
     * Query source information based on inlong group id and inlong stream id.
     *
     * @param groupId Inlong group id.
     * @param streamId Inlong stream id, can be null.
     * @return Source info list.
     */
    List<SourceResponse> listSource(String groupId, String streamId);

    /**
     * Query the number of undeleted source info based on inlong group and inlong stream id.
     *
     * @param groupId Inlong group id.
     * @param streamId Inlong stream id.
     * @return Number of source info.
     */
    Integer getCount(String groupId, String streamId);

    /**
     * Paging query source information based on conditions.
     *
     * @param request Paging request.
     * @return Source info list.
     */
    PageInfo<? extends SourceListResponse> listByCondition(SourcePageRequest request);

    /**
     * Modify data source information
     *
     * @param sourceRequest Information that needs to be modified
     * @param operator Operator's name
     * @return whether succeed
     */
    boolean update(SourceRequest sourceRequest, String operator);

    /**
     * Delete the stream source by the given id and source type.
     *
     * @param id The primary key of the source.
     * @param sourceType Source type.
     * @param operator Operator's name
     * @return Whether succeed
     */
    boolean delete(Integer id, String sourceType, String operator);

    /**
     * Modify source data status.
     *
     * @param id Source id.
     * @param status Target status.
     * @param log Modify the log.
     */
    void updateStatus(int id, int status, String log);

    /**
     * Logically delete stream source with the given conditions.
     *
     * @param groupId InLong group id to which the data source belongs.
     * @param streamId InLong stream id to which the data source belongs.
     * @param operator Operator's name
     * @return Whether succeed.
     */
    boolean logicDeleteAll(String groupId, String streamId, String operator);

    /**
     * Physically delete stream source with the given conditions.
     *
     * @param groupId InLong group id.
     * @param streamId InLong stream id.
     * @param operator Operator's name
     * @return Whether succeed.
     */
    boolean deleteAll(String groupId, String streamId, String operator);

    /**
     * According to the inlong stream id, query the list of source types owned by it.
     *
     * @param groupId Inlong group id.
     * @param streamId Inlong stream id.
     * @return List of source types.
     */
    List<String> getSourceTypeList(String groupId, String streamId);

    /**
     * Save the information modified when the approval is passed.
     *
     * @param operator Operator's name
     * @return Whether succeed.
     */
    default Boolean updateAfterApprove(String operator) {
        return true;
    }

    /**
     * Report the heartbeat for given source.
     *
     * @param request Heartbeat request.
     * @return Whether succeed.
     */
    Boolean reportSnapshot(SourceSnapshotRequest request);

}
