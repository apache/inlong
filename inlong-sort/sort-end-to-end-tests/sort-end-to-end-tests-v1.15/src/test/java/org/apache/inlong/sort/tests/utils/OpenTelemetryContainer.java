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

package org.apache.inlong.sort.tests.utils;

import org.testcontainers.containers.GenericContainer;

public class OpenTelemetryContainer extends GenericContainer {

    public static final String IMAGE = "otel/opentelemetry-collector-contrib:0.110.0";
    public static final Integer PORT = 4317;
    public OpenTelemetryContainer() {
        super(IMAGE);
        addExposedPort(PORT);
    }
}
