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

package org.apache.inlong.manager.client.cli;

import com.google.gson.Gson;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.common.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.cli.util.GsonUtil;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Util of command for creat connect by config file.
 */
abstract class CommandUtil {

    private static final String CONFIG_FILE = "application.properties";

    public InlongClientImpl connect() {
        Properties properties = new Properties();
        String path = Thread.currentThread().getContextClassLoader().getResource("").getPath() + CONFIG_FILE;
        try {
            InputStream inputStream = new BufferedInputStream(Files.newInputStream(Paths.get(path)));
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String serviceUrl = properties.getProperty("server.host") + ":" + properties.getProperty("server.port");
        String user = properties.getProperty("default.admin.user");
        String password = properties.getProperty("default.admin.password");

        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setAuthentication(new DefaultAuthentication(user, password));

        return new InlongClientImpl(serviceUrl, configuration);
    }

    String readFile(File file) {
        if (!file.exists()) {
            System.out.println("File does not exist.");
        } else {
            try {
                FileReader fileReader = new FileReader(file);
                Reader reader = new InputStreamReader(Files.newInputStream(file.toPath()));
                int ch;
                StringBuilder stringBuilder = new StringBuilder();
                while ((ch = reader.read()) != -1) {
                    stringBuilder.append((char) ch);
                }
                fileReader.close();
                reader.close();

                return stringBuilder.toString();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        return null;
    }

    CreateGroupConf jsonToObject(String string) {
        Gson gson = GsonUtil.gsonBuilder();
        return gson.fromJson(string, CreateGroupConf.class);
    }

    abstract void run() throws Exception;
}
