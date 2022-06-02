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

package org.apache.inlong.manager.client.cli.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.enums.SimpleGroupStatus;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Util that prints information to the console.
 */
public class PrintUtils {

    private static final String joint = "+";
    private static final String horizontal = "—";
    private static final String vertical = "|";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static <T, K> void print(List<T> item, Class<K> clazz) {
        if (item.isEmpty()) {
            return;
        }
        List<K> list = copyObject(item, clazz);
        int[] maxColumnWidth = getColumnWidth(list);
        printTable(list, maxColumnWidth);
    }

    public static <T> void printJson(T item) {
        try {
            System.out.println(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(item));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static <K> void printTable(List<K> list, int[] columnWidth) {
        Field[] fields = list.get(0).getClass().getDeclaredFields();

        String format = "%s" + vertical;
        printLine(columnWidth, fields.length);
        System.out.print(vertical);
        for (int i = 0; i < fields.length; i++) {
            System.out.printf(format, StringUtils.center(fields[i].getName(), columnWidth[i]));
        }
        System.out.println();
        printLine(columnWidth, fields.length);
        list.forEach(k -> {
            for (int i = 0; i < fields.length; i++) {
                fields[i].setAccessible(true);
                try {
                    System.out.print(vertical);
                    if (fields[i].get(k) != null) {
                        int charNum = getSpecialCharNum(fields[i].get(k).toString());
                        if (fields[i].getType().equals(Date.class)) {
                            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            String dataFormat = sf.format(fields[i].get(k));
                            System.out.printf("%s", StringUtils.center(dataFormat, columnWidth[i]));
                        } else if (charNum > 0) {
                            System.out.printf("%s",
                                    StringUtils.center(fields[i].get(k).toString(), columnWidth[i] - charNum));
                        } else {
                            System.out.printf("%s", StringUtils.center(fields[i].get(k).toString(), columnWidth[i]));
                        }
                    } else {
                        System.out.printf("%s", StringUtils.center("NULL", columnWidth[i]));
                    }
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            System.out.println(vertical);
        });
        printLine(columnWidth, fields.length);
    }

    public static <T, K> List<K> copyObject(List<T> item, Class<K> clazz) {
        List<K> newList = new ArrayList<>();
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        item.forEach(t -> {
            try {
                K k = OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(t), clazz);
                parseStatus(k);
                newList.add(k);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
        return newList;
    }

    private static <K> int[] getColumnWidth(List<K> list) {
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
                        int length = fields[j].get(k).toString().getBytes().length;
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

    private static void printLine(int[] columnWidth, int fieldNum) {
        System.out.print(joint);
        for (int i = 0; i < fieldNum; i++) {
            System.out.printf("%s", StringUtils.leftPad(joint, columnWidth[i] + 1, horizontal));
        }
        System.out.println();
    }

    private static int getSpecialCharNum(String str) {
        int i = str.getBytes().length - str.length();
        return i / 2;
    }

    private static <T> void parseStatus(T t) {
        Field[] fields = t.getClass().getDeclaredFields();
        for (Field f : fields) {
            f.setAccessible(true);
            if (f.isAnnotationPresent(ParseStatus.class)) {
                try {
                    int status = Integer.parseInt(f.get(t).toString());
                    SimpleGroupStatus groupStatus = SimpleGroupStatus.parseStatusByCode(status);
                    f.set(t, String.format("%s (%s)", groupStatus, status));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
