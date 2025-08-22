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

package org.apache.inlong.agent.installer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * check manager interface result with json formatter.
 */
public class ManagerResultFormatter {

    public static final String SUCCESS_CODE = "true";
    private static final Logger LOGGER = LoggerFactory.getLogger(ManagerResultFormatter.class);
    private static final String RESULT_CODE = "success";
    private static final String RESULT_DATA = "data";
    private static final Gson GSON = new Gson();

    /**
     * get json result
     *
     * @return json object
     */
    public static JsonObject getResultData(String jsonStr) {
        JsonObject object = GSON.fromJson(jsonStr, JsonObject.class);
        if (object == null || !object.has(RESULT_CODE) || !object.has(RESULT_DATA)
                || !SUCCESS_CODE.equals(object.get(RESULT_CODE).getAsString())) {
            LOGGER.warn("cannot get result data, please check manager status, return str is {}", jsonStr);
        }
        return object;
    }
}
