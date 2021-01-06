/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.manager.utils;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ConvertUtils {

    public static Gson gson = new Gson();

    public static String convertReqToQueryStr(Object req) throws Exception {
        List<String> queryList = new ArrayList<>();
        Class<?> clz = req.getClass();
        List fieldsList = new ArrayList<Field[]>();

        while (clz != null) {
            Field[] declaredFields = clz.getDeclaredFields();
            fieldsList.add(declaredFields);
            clz = clz.getSuperclass();
        }

        for (Object fields:fieldsList) {
            Field[] f = (Field[]) fields;
            for (Field field : f) {
                field.setAccessible(true);
                Object o = field.get(req);
                String value;
                // convert list to json string
                if (o == null) {
                    continue;
                }
                if (o instanceof List) {
                    value = gson.toJson(o);
                } else {
                    value = o.toString();
                }
                queryList.add(field.getName() + "=" + URLEncoder.encode(
                    value, UTF_8.toString()));
            }
        }

        return StringUtils.join(queryList, "&");
    }
}
