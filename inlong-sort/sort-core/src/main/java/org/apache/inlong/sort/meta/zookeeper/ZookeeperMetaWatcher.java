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

package org.apache.inlong.sort.meta.zookeeper;

import org.apache.inlong.sort.configuration.Configuration;
import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.meta.MetaWatcher;

/**
 * 
 * ZookeeperMetaWatcher
 */
public class ZookeeperMetaWatcher implements MetaWatcher {

    private ZookeeperWatcherUtils watcherUtils;

    /**
     * Constructor
     */
    public ZookeeperMetaWatcher() {
        this.watcherUtils = new ZookeeperWatcherUtils();
    }

    /**
     * open
     * 
     * @param  config
     * @param  metaListener
     * @throws Exception
     */
    public void open(Configuration config, DataFlowInfoListener metaListener) throws Exception {
        this.watcherUtils.open(config, metaListener);
    }

    @Override
    public void close() throws Exception {
        this.watcherUtils.close();
    }
}
