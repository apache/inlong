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

package org.apache.inlong.manager.service.module;

import org.apache.inlong.manager.pojo.module.ModuleDTO;
import org.apache.inlong.manager.pojo.module.ModuleRequest;

import java.util.Arrays;
import java.util.List;

/**
 * Flattens a carrier ({@link ModuleRequest} / {@link ModuleDTO} / ...) into a plain list
 * of raw command strings.
 */
public final class ModuleCommandAccessors {

    private ModuleCommandAccessors() {
    }

    public static List<String> of(ModuleRequest request) {
        return Arrays.asList(
                request.getStartCommand(),
                request.getStopCommand(),
                request.getCheckCommand(),
                request.getInstallCommand(),
                request.getUninstallCommand());
    }

    public static List<String> of(ModuleDTO dto) {
        return Arrays.asList(
                dto.getStartCommand(),
                dto.getStopCommand(),
                dto.getCheckCommand(),
                dto.getInstallCommand(),
                dto.getUninstallCommand());
    }
}
