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

/**
 * The five command fields validated by {@link ModuleCommandValidator}. Only carries the
 * display label used in error messages; how to read the value from a given carrier
 * (DTO / Request / ...) is supplied by {@link ModuleCommandAccessors}, so this enum is
 * decoupled from any concrete POJO type.
 *
 * <p>Package-private on purpose — it is an implementation detail shared between
 * {@link ModuleCommandValidator} and {@link ModuleCommandAccessors} and should not leak
 * to callers.
 */
enum CommandField {

    START("startCommand"),
    STOP("stopCommand"),
    CHECK("checkCommand"),
    INSTALL("installCommand"),
    UNINSTALL("uninstallCommand");

    final String label;

    CommandField(String label) {
        this.label = label;
    }
}
