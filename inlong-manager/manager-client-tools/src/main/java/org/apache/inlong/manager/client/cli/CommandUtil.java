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
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.auth.DefaultAuthentication;
import org.apache.inlong.manager.client.api.impl.InlongClientImpl;
import org.apache.inlong.manager.client.cli.util.GsonUtil;
import org.springframework.beans.BeanUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

abstract class CommandUtil {

    public InlongClientImpl connect() {
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

        return new InlongClientImpl(serviceUrl, configuration);
    }

    <T, K> void print(List<T> item, Class<K> clazz) {
        if (item.isEmpty()) {
            return;
        }
        List<K> list = copyObject(item, clazz);
        int[] maxColumnWidth = getColumnWidth(list);
        printTable(list, maxColumnWidth);
    }

    <T> void printJson(T item) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = jsonParser.parse(gson.toJson(item)).getAsJsonObject();
        System.out.println(gson.toJson(jsonObject));
    }

    String readFile(File file) {
        if (!file.exists()) {
            System.out.println("File does not exist.");
        } else {
            try {
                FileReader fileReader = new FileReader(file);
                Reader reader = new InputStreamReader(new FileInputStream(file));
                int ch;
                StringBuffer stringBuffer = new StringBuffer();
                while ((ch = reader.read()) != -1) {
                    stringBuffer.append((char) ch);
                }
                fileReader.close();
                reader.close();

                return stringBuffer.toString();
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

    private <K> void printTable(List<K> list, int[] columnWidth) {
        Field[] fields = list.get(0).getClass().getDeclaredFields();
        System.out.print("|");
        for (int i = 0; i < fields.length; i++) {
            System.out.printf("%s|", StringUtils.center(fields[i].getName(), columnWidth[i]));
        }
        System.out.println();
        for (int i = 0; i < fields.length; i++) {
            System.out.printf("%s", StringUtils.leftPad("—", columnWidth[i] + 1, "—"));
        }
        System.out.println();
        list.forEach(k -> {
            for (int j = 0; j < fields.length; j++) {
                fields[j].setAccessible(true);
                try {
                    System.out.print("|");
                    if (fields[j].get(k) != null) {
                        if (fields[j].getType().equals(Date.class)) {
                            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            String dataFormat = sf.format(fields[j].get(k));
                            System.out.printf("%s", StringUtils.center(dataFormat, columnWidth[j]));
                        } else {
                            System.out.printf("%s", StringUtils.center(fields[j].get(k).toString(), columnWidth[j]));
                        }
                    } else {
                        System.out.printf("%s", StringUtils.center("NULL", columnWidth[j]));
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("|");
        });
    }

    private <T, K> List<K> copyObject(List<T> item, Class<K> clazz) {
        List<K> newList = new ArrayList<>();
        item.forEach(t -> {
            try {
                K k = clazz.newInstance();
                BeanUtils.copyProperties(t, k);
                newList.add(k);
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        });
        return newList;
    }

    private <K> int[] getColumnWidth(List<K> list) {
        Field[] fields = list.get(0).getClass().getDeclaredFields();
        int[] maxWidth = new int[fields.length];
        for (int i = 0; i < fields.length; i++) {
            maxWidth[i] = Math.max(fields[i].getName().length(), maxWidth[i]);
        }
        list.forEach(k -> {
            try {
                for (int j = 0; j < fields.length; j++) {
                    fields[j].setAccessible(true);
                    if (fields[j].get(k) != null) {
                        int length = fields[j].get(k).toString().length();
                        maxWidth[j] = Math.max(length, maxWidth[j]);
                    }
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        });
        for (int i = 0; i < maxWidth.length; i++) {
            maxWidth[i] += 4;
        }
        return maxWidth;
    }
}
