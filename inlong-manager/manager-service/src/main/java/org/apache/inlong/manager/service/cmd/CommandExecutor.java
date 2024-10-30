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

package org.apache.inlong.manager.service.cmd;

import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterNodeRequest;

import java.util.Map;

public interface CommandExecutor {

    CommandResult exec(String cmd) throws Exception;

    CommandResult execSSHKeyGeneration() throws Exception;

    CommandResult execRemote(AgentClusterNodeRequest clusterNodeRequest, String cmd) throws Exception;

    CommandResult modifyConfig(AgentClusterNodeRequest clusterNodeRequest, Map<String, String> configMap,
            String confPath) throws Exception;

    CommandResult tarPackage(AgentClusterNodeRequest clusterNodeRequest, String fileName, String sourcePath,
            String tarPath) throws Exception;

    CommandResult downLoadPackage(AgentClusterNodeRequest clusterNodeRequest, String downLoadPath, String downLoadUrl)
            throws Exception;

    CommandResult mkdir(AgentClusterNodeRequest clusterNodeRequest, String path) throws Exception;

    CommandResult rmDir(AgentClusterNodeRequest clusterNodeRequest, String path) throws Exception;

    CommandResult cpDir(AgentClusterNodeRequest clusterNodeRequest, String sourcePath, String targetPath)
            throws Exception;

}
