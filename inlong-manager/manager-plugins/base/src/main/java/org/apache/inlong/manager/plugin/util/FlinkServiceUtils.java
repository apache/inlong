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

package org.apache.inlong.manager.plugin.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;

@Slf4j
public class FlinkServiceUtils {

    private static final String FLINK_CLIENT_SERVICE_PATH = "file://xxx.jar";
    private static final String CLASS_PATH = "org.apache.inlong.manager.plugin.flink.FlinkClientService";

    public static Object getFlinkClientService(Configuration configuration) {
        Class<?> flinkClientService;
        try (URLClassLoader classLoader = new URLClassLoader(new URL[]{new URL(FLINK_CLIENT_SERVICE_PATH)})) {
            flinkClientService = classLoader.loadClass(CLASS_PATH);
            Constructor<?> con = flinkClientService.getDeclaredConstructor(Configuration.class);
            return con.newInstance(configuration);
        } catch (IOException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException
                | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
