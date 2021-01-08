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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import org.apache.tubemq.manager.controller.topic.request.RebalanceConsumerReq;
import org.apache.tubemq.manager.controller.topic.request.RebalanceGroupReq;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.OP_MODIFY;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.OP_QUERY;
import static org.apache.tubemq.manager.service.TubeMQHttpConst.REBALANCE_GROUP;

@Slf4j
public class ConvertUtils {

    public static Gson gson = new Gson();

    public static String convertReqToQueryStr(Object req) {
        List<String> queryList = new ArrayList<>();
        Class<?> clz = req.getClass();
        List fieldsList = new ArrayList<Field[]>();

        while (clz != null) {
            Field[] declaredFields = clz.getDeclaredFields();
            fieldsList.add(declaredFields);
            clz = clz.getSuperclass();
        }

        try {
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
        } catch (Exception e) {
            log.error("exception occurred while parsing object {}", gson.toJson(req), e);
            return StringUtils.EMPTY;
        }

        return StringUtils.join(queryList, "&");
    }


    public static RebalanceConsumerReq convertToRebalanceConsumerReq(RebalanceGroupReq req, String consumerId) {
        RebalanceConsumerReq consumerReq = new RebalanceConsumerReq();
        consumerReq.setConsumerId(consumerId);
        consumerReq.setConfModAuthToken(req.getConfModAuthToken());
        consumerReq.setGroupName(req.getGroupName());
        consumerReq.setModifyUser(req.getModifyUser());
        consumerReq.setReJoinWait(req.getReJoinWait());
        consumerReq.setType(OP_MODIFY);
        consumerReq.setMethod(REBALANCE_GROUP);
        return consumerReq;
    }
}
