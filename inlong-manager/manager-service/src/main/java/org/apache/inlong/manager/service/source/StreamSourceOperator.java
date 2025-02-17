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

import org.apache.inlong.common.pojo.agent.DataConfig;
import org.apache.inlong.manager.dao.entity.InlongStreamEntity;
import org.apache.inlong.manager.dao.entity.StreamSourceEntity;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.group.InlongGroupInfo;
import org.apache.inlong.manager.pojo.source.DataAddTaskRequest;
import org.apache.inlong.manager.pojo.source.SourceRequest;
import org.apache.inlong.manager.pojo.source.StreamSource;
import org.apache.inlong.manager.pojo.stream.InlongStreamInfo;
import org.apache.inlong.manager.pojo.stream.StreamField;

import com.github.pagehelper.Page;

import javax.validation.constraints.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Interface of the source operator
 */
public interface StreamSourceOperator {

    /**
     * Determines whether the current instance matches the specified type.
     */
    Boolean accept(String sourceType);

    String getExtParams(StreamSourceEntity sourceEntity);

    /**
     * Save the source info.
     *
     * @param request request of source
     * @param streamStatus the belongs stream status
     * @param operator name of operator
     * @return source id after saving
     */
    Integer saveOpt(SourceRequest request, Integer streamStatus, String operator);

    /**
     * Get source info by the given entity.
     *
     * @param entity get field value from the entity
     * @return source info
     */
    StreamSource getFromEntity(StreamSourceEntity entity);

    /**
     * Get stream source field list by the given source id.
     *
     * @param sourceId source id
     * @return stream field list
     */
    List<StreamField> getSourceFields(@NotNull Integer sourceId);

    /**
     * Get the StreamSource Map by the inlong group info and inlong stream info list.
     *
     * @param groupInfo inlong group info
     * @param streamInfos inlong stream info list
     * @param streamSources stream source list
     * @return map of StreamSource list, key-inlongStreamId, value-StreamSourceList
     * @apiNote The MQ source which was used in InlongGroup must implement the method.
     */
    default Map<String, List<StreamSource>> getSourcesMap(InlongGroupInfo groupInfo,
            List<InlongStreamInfo> streamInfos, List<StreamSource> streamSources) {
        return new HashMap<>();
    }

    /**
     * Get source list response from the given source entity page.
     *
     * @param entityPage given entity page
     * @return source list response
     */
    PageResult<? extends StreamSource> getPageInfo(Page<StreamSourceEntity> entityPage);

    /**
     * Update the source info.
     *
     * @param request request of source
     * @param groupStatus the belongs group status
     * @param operator name of operator
     */
    void updateOpt(SourceRequest request, Integer groupStatus, Integer groupMode, String operator);

    /**
     * Stop the source task.
     *
     * @param request request of source
     * @param operator name of operator
     */
    void stopOpt(SourceRequest request, String operator);

    /**
     * Restart the source task.
     *
     * @param request request of source
     * @param operator name of operator
     */
    void restartOpt(SourceRequest request, String operator);

    /**
     * Sync the source field info to stream fields.
     *
     * @param request request of source
     * @param operator operator
     */
    void syncSourceFieldInfo(SourceRequest request, String operator);

    /**
     * Save the data add task info.
     *
     * @param request request of data add task
     * @param operator name of operator
     * @return source id after saving
     */
    Integer addDataAddTask(DataAddTaskRequest request, String operator);

    /**
     * Update the agent task config info.
     *
     * @param request source request
     * @param operator name of the operator
     */
    void updateAgentTaskConfig(SourceRequest request, String operator);

    default String updateDataConfig(String extParams, InlongStreamEntity streamEntity, DataConfig dataConfig) {
        return extParams;
    }

}
