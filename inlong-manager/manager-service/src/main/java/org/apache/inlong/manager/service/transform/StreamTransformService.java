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

package org.apache.inlong.manager.service.transform;

import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.sink.StreamSink;
import org.apache.inlong.manager.pojo.transform.DeleteTransformRequest;
import org.apache.inlong.manager.pojo.transform.TransformPageRequest;
import org.apache.inlong.manager.pojo.transform.TransformRequest;
import org.apache.inlong.manager.pojo.transform.TransformResponse;
import org.apache.inlong.manager.pojo.user.UserInfo;

import java.util.List;

/**
 * Service layer interface for stream transform
 */
public interface StreamTransformService {

    /**
     * Save the transform information.
     *
     * @param request the transform request
     * @param operator name of the operator
     * @return transform id after saving
     */
    Integer save(TransformRequest request, String operator);

    /**
     * Query transform information based on inlong group id and inlong stream id.
     *
     * @param request the transform page request
     * @return the transform response
     */
    PageResult<TransformResponse> listByCondition(TransformPageRequest request, UserInfo opInfo);

    /**
     * Query transform information based on id
     *
     * @param id transform id.
     * @param opInfo userinfo of operator
     * @return transform info
     */
    TransformResponse get(Integer id, UserInfo opInfo);

    /**
     * Query transform information based on inlong group id and inlong stream id.
     *
     * @param groupId the inlong group id
     * @param streamId the inlong stream id
     * @return the transform response
     */
    List<TransformResponse> listTransform(String groupId, String streamId);

    /**
     * Modify data transform information.
     *
     * @param request the transform request
     * @param operator name of the operator
     * @return Whether succeed
     */
    Boolean update(TransformRequest request, String operator);

    /**
     * Delete the stream transform by the given id.
     *
     * @param request delete request
     * @param operator name of the operator
     * @return Whether succeed
     */
    Boolean delete(DeleteTransformRequest request, String operator);

    /**
     * Get transform sql by sink
     *
     * @param sink sink info
     * @return return transform sql
     */
    String getTransformSql(StreamSink sink);

    /**
     * Parse transform sql by transform information
     *
     * @param request the transform request
     * @return return transform sql
     */
    String parseTransformSql(TransformRequest request, String operator);

}
