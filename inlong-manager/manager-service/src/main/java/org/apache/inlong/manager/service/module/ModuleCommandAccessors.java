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

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;

/**
 * The single place in this package that knows how to read the five command fields off a
 * concrete POJO. Given a carrier ({@link ModuleRequest} / {@link ModuleDTO} / ...), it
 * returns a {@code Function<CommandField, String>} that {@link ModuleCommandValidator}
 * can drive uniformly.
 *
 * <p>Design notes:
 * <ul>
 *   <li>The field \u2192 getter mapping is registered <b>once</b> per carrier type in a
 *       {@link EnumMap} \u2014 no more per-carrier {@code switch} statements.</li>
 *   <li>Adding a new carrier (e.g. {@code ModuleResponse}) = one extra static map + one
 *       extra {@code of(...)} overload. Nothing in {@link ModuleCommandValidator}
 *       changes.</li>
 *   <li>Package-private on purpose \u2014 this is an internal helper, not part of any
 *       public API.</li>
 * </ul>
 */
public final class ModuleCommandAccessors {

    private static final Map<CommandField, Function<ModuleRequest, String>> REQUEST_GETTERS =
            new EnumMap<>(CommandField.class);
    private static final Map<CommandField, Function<ModuleDTO, String>> DTO_GETTERS =
            new EnumMap<>(CommandField.class);

    static {
        REQUEST_GETTERS.put(CommandField.START, ModuleRequest::getStartCommand);
        REQUEST_GETTERS.put(CommandField.STOP, ModuleRequest::getStopCommand);
        REQUEST_GETTERS.put(CommandField.CHECK, ModuleRequest::getCheckCommand);
        REQUEST_GETTERS.put(CommandField.INSTALL, ModuleRequest::getInstallCommand);
        REQUEST_GETTERS.put(CommandField.UNINSTALL, ModuleRequest::getUninstallCommand);

        DTO_GETTERS.put(CommandField.START, ModuleDTO::getStartCommand);
        DTO_GETTERS.put(CommandField.STOP, ModuleDTO::getStopCommand);
        DTO_GETTERS.put(CommandField.CHECK, ModuleDTO::getCheckCommand);
        DTO_GETTERS.put(CommandField.INSTALL, ModuleDTO::getInstallCommand);
        DTO_GETTERS.put(CommandField.UNINSTALL, ModuleDTO::getUninstallCommand);
    }

    private ModuleCommandAccessors() {
    }

    /**
     * Return a reader over the command fields of the given {@link ModuleRequest}.
     */
    static Function<CommandField, String> of(ModuleRequest request) {
        return readerFor(request, REQUEST_GETTERS);
    }

    /**
     * Return a reader over the command fields of the given {@link ModuleDTO}.
     */
    static Function<CommandField, String> of(ModuleDTO dto) {
        return readerFor(dto, DTO_GETTERS);
    }

    private static <T> Function<CommandField, String> readerFor(T carrier,
            Map<CommandField, Function<T, String>> getters) {
        return field -> {
            Function<T, String> getter = getters.get(field);
            if (getter == null) {
                throw new IllegalStateException("No getter registered for command field: " + field);
            }
            return getter.apply(carrier);
        };
    }
}
