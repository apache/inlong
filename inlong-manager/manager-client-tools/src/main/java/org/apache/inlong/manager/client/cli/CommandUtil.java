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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

abstract class CommandUtil {

    public InnerInlongManagerClient connect() {
        Properties properties = new Properties();
        String path = System.getProperty("user.dir") + "/conf/application.properties";

        try {
            InputStream inputStream = new BufferedInputStream(new FileInputStream(path));
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String serviceUrl = properties.getProperty("server.host") + ":" + properties.getProperty("server.port");
        String user = properties.getProperty("default.admin.user");
        String password = properties.getProperty("default.admin.password");

        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setAuthentication(new DefaultAuthentication(user, password));
        InlongClientImpl inlongClient = new InlongClientImpl(serviceUrl, configuration);
        return new InnerInlongManagerClient(inlongClient.getConfiguration());
    }

    <T> void print(List<T> item, Class<T> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field f : fields) {
            System.out.printf("%-30s", f.getName());
        }
        System.out.println();
        if (!item.isEmpty()) {
            item.forEach(t -> {
                try {
                    for (Field f : fields) {
                        f.setAccessible(true);
                        if (f.get(t) != null) {
                            System.out.printf("%-30s", f.get(t).toString());
                        } else {
                            System.out.printf("%-30s", "NULL");
                        }
                    }
                    System.out.println();
                } catch (IllegalAccessException e) {
                    System.out.println(e.getMessage());
                }
            });
        }
    }

    <T> void printJson(T item) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = jsonParser.parse(gson.toJson(item)).getAsJsonObject();
        System.out.println(gson.toJson(jsonObject));
    }

    abstract void run() throws Exception;
}
