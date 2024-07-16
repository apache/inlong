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

package org.apache.inlong.common.pojo.agent.installer;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The Module config for installer.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ModuleConfig {

    private Integer id;
    /**
     * The primary key ID of the manager module config table
     */
    private Integer entityId;
    private String name;
    /**
     * The md5 of the module config
     */
    private String md5;
    private String version;
    /**
     * Number of processes in one node
     */
    private Integer processesNum;
    /**
     * The command to start the module
     */
    private String startCommand;
    /**
     * The command to stop the module
     */
    private String stopCommand;
    /**
     * The command to check the processes num of the module
     */
    private String checkCommand;
    /**
     * The command to install the module
     */
    private String installCommand;
    /**
     * The command to uninstall the module
     */
    private String uninstallCommand;
    /**
     * Installation package config
     */
    private PackageConfig packageConfig;
    /**
     * The state of the module，identify that the module is in a state of addition, download, installation, etc
     */
    private ModuleStateEnum state;

    /**
     * The restart time of the module
     */
    private Integer restartTime;

}