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

package org.apache.inlong.manager.service.sink;

import com.github.pagehelper.PageInfo;
import org.apache.inlong.manager.common.pojo.sink.SinkApproveDTO;
import org.apache.inlong.manager.common.pojo.sink.SinkBriefResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.sink.SinkPageRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkResponse;

import java.util.List;

/**
 * Service layer interface for stream sink
 */
public interface StreamSinkService {

    /**
     * Save the sink information
     *
     * @param request Sink request.
     * @param operator Operator's name.
     * @return sink id after saving.
     */
    Integer save(SinkRequest request, String operator);

    /**
     * Query sink information based on id and type.
     *
     * @param id sink id.
     * @param sinkType Sink type.
     * @return Store info
     */
    SinkResponse get(Integer id, String sinkType);

    /**
     * Query sink information based on inlong group id and inlong stream id
     *
     * @param groupId Inlong group id
     * @param streamId Inlong stream id, can be null
     * @return Store information list
     * @apiNote Sink types only support temporarily: HIVE
     */
    List<SinkResponse> listSink(String groupId, String streamId);

    /**
     * Query stored summary information based on inlong group id and inlong stream id, including sink cluster
     *
     * @param groupId Inlong group id
     * @param streamId Inlong stream id
     * @return Store information list
     * @apiNote Sink types only support temporarily: HIVE
     */
    List<SinkBriefResponse> listBrief(String groupId, String streamId);

    /**
     * Query the number of undeleted stored information based on inlong group and inlong stream id
     *
     * @param groupId Inlong group id
     * @param streamId Inlong stream id
     * @return Number of stored information
     */
    Integer getCount(String groupId, String streamId);

    /**
     * Paging query sink information based on conditions
     *
     * @param request Paging request
     * @return Store information list
     */
    PageInfo<? extends SinkListResponse> listByCondition(SinkPageRequest request);

    /**
     * Modify data sink information
     *
     * @param sinkRequest Information that needs to be modified
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean update(SinkRequest sinkRequest, String operator);

    /**
     * Delete the stream sink by the given id and sink type.
     *
     * @param id The primary key of the sink.
     * @param sinkType Sink type.
     * @param operator The operator's name.
     * @return Whether succeed
     */
    boolean delete(Integer id, String sinkType, String operator);

    /**
     * Modify sink data status
     *
     * @param id Stored id
     * @param status Goal status
     * @param log Modify the description
     */
    void updateStatus(int id, int status, String log);

    /**
     * Logically delete stream sink with the given conditions.
     *
     * @param groupId InLong group id to which the data source belongs.
     * @param streamId InLong stream id to which the data source belongs.
     * @param operator The operator's name.
     * @return Whether succeed.
     */
    boolean logicDeleteAll(String groupId, String streamId, String operator);

    /**
     * Physically delete stream sink with the given conditions.
     *
     * @param groupId InLong group id.
     * @param streamId InLong stream id.
     * @param operator The operator's name.
     * @return Whether succeed.
     */
    boolean deleteAll(String groupId, String streamId, String operator);

    /**
     * According to the existing inlong stream ID list, filter out the inlong stream ID list containing the specified
     * sink type
     *
     * @param groupId Inlong group id
     * @param sinkType Sink type
     * @param streamIdList Inlong stream ID list
     * @return List of filtered inlong stream IDs
     */
    List<String> getExistsStreamIdList(String groupId, String sinkType, List<String> streamIdList);

    /**
     * According to the inlong stream id, query the list of sink types owned by it
     *
     * @param groupId Inlong group id
     * @param streamId Inlong stream id
     * @return List of sink types
     */
    List<String> getSinkTypeList(String groupId, String streamId);

    /**
     * Save the information modified when the approval is passed
     *
     * @param sinkApproveList Stream sink approval information
     * @param operator Edit person's name
     * @return whether succeed
     */
    boolean updateAfterApprove(List<SinkApproveDTO> sinkApproveList, String operator);

}
