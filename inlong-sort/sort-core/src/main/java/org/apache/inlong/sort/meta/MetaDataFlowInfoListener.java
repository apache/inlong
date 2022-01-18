/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.meta;

import org.apache.inlong.sort.meta.MetaManager.DataFlowInfoListener;
import org.apache.inlong.sort.protocol.DataFlowInfo;

/**
 * 
 * MetaDataFlowInfoListener
 */
public class MetaDataFlowInfoListener implements DataFlowInfoListener {

    private MetaManager manager;

    /**
     * Constructor
     * 
     * @param manager
     */
    public MetaDataFlowInfoListener(MetaManager manager) {
        this.manager = manager;
    }

    /**
     * addDataFlow
     * 
     * @param  dataFlowInfo
     * @throws Exception
     */
    @Override
    public void addDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        this.manager.addDataFlow(dataFlowInfo);
    }

    /**
     * updateDataFlow
     * 
     * @param  dataFlowInfo
     * @throws Exception
     */
    @Override
    public void updateDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        this.manager.updateDataFlow(dataFlowInfo);
    }

    /**
     * removeDataFlow
     * 
     * @param  dataFlowInfo
     * @throws Exception
     */
    @Override
    public void removeDataFlow(DataFlowInfo dataFlowInfo) throws Exception {
        this.manager.removeDataFlow(dataFlowInfo);
    }

}
