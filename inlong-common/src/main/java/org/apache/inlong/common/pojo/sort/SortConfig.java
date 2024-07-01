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

package org.apache.inlong.common.pojo.sort;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SortConfig implements Serializable {

    private String sortClusterName;
    private List<TaskConfig> tasks;

    public static SortConfig checkLatest(SortConfig last, SortConfig current) {
        if (last == null) {
            return current;
        }
        if (current == null) {
            return null;
        }
        return SortConfig.builder()
                .sortClusterName(current.getSortClusterName())
                .tasks(TaskConfig.batchCheckLatest(last.getTasks(), current.getTasks()))
                .build();
    }

    public static SortConfig checkDelete(SortConfig last, SortConfig current) {
        if (last == null) {
            return null;
        }
        if (current == null) {
            return last;
        }
        return check(last, current, TaskConfig::batchCheckDelete);
    }

    public static SortConfig checkUpdate(SortConfig last, SortConfig current) {
        if (last == null || current == null) {
            return null;
        }
        return check(last, current, TaskConfig::batchCheckUpdate);
    }

    public static SortConfig checkNew(SortConfig last, SortConfig current) {
        if (last == null) {
            return current;
        }
        if (current == null) {
            return null;
        }
        return check(last, current, TaskConfig::batchCheckNew);
    }

    public static SortConfig check(
            SortConfig last, SortConfig current,
            BiFunction<List<TaskConfig>, List<TaskConfig>, List<TaskConfig>> taskCheckFunction) {
        if (!last.getSortClusterName().equals(current.getSortClusterName())) {
            return null;
        }

        List<TaskConfig> checkTasks = taskCheckFunction.apply(last.getTasks(), current.getTasks());
        if (CollectionUtils.isEmpty(checkTasks)) {
            return null;
        }

        return SortConfig.builder()
                .sortClusterName(last.getSortClusterName())
                .tasks(checkTasks)
                .build();
    }
}
