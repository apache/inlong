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

package org.apache.inlong.manager.workflow.dao;

import java.util.List;
import org.apache.ibatis.annotations.Param;
import org.apache.inlong.manager.workflow.model.TaskState;
import org.apache.inlong.manager.workflow.model.instance.TaskInstance;
import org.apache.inlong.manager.workflow.model.view.CountByKey;
import org.apache.inlong.manager.workflow.model.view.TaskQuery;
import org.apache.inlong.manager.workflow.model.view.TaskSummaryQuery;

/**
 * Task instance
 */
public interface TaskInstanceStorage {

    int insert(TaskInstance taskInstance);

    int update(TaskInstance taskInstance);

    TaskInstance get(Integer id);

    List<TaskInstance> list(@Param("processInstId") Integer processInstId, @Param("state") TaskState state);

    int countTask(@Param("processInstId") Integer processInstId, @Param("name") String name,
            @Param("state") TaskState state);

    List<TaskInstance> listByQuery(TaskQuery query);

    List<CountByKey> countByState(TaskSummaryQuery query);

}
