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

package org.apache.inlong.manager.plugin;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.inlong.manager.common.event.EventSelector;
import org.apache.inlong.manager.common.event.task.DataSourceOperateListener;
import org.apache.inlong.manager.common.event.task.QueueOperateListener;
import org.apache.inlong.manager.common.event.task.SortOperateListener;
import org.apache.inlong.manager.common.event.task.StorageOperateListener;
import org.apache.inlong.manager.common.plugin.ProcessPlugin;

public class EmptyProcessPlugin implements ProcessPlugin {

    @Override
    public Map<DataSourceOperateListener, EventSelector> createSourceOperateListeners() {
        return new LinkedHashMap<>();
    }

    @Override
    public Map<QueueOperateListener, EventSelector> createQueueOperateListeners() {
        return new LinkedHashMap<>();
    }

    @Override
    public Map<SortOperateListener, EventSelector> createSortOperateListeners() {
        return ProcessPlugin.super.createSortOperateListeners();
    }

    @Override
    public Map<StorageOperateListener, EventSelector> createStorageOperateListeners() {
        return ProcessPlugin.super.createStorageOperateListeners();
    }
}
