/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.standalone.sink.cls;

import com.tencentcloudapi.cls.producer.common.LogItem;
import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.utils.InlongLoggerFactory;
import org.apache.inlong.sort.standalone.utils.UnescapeHelper;
import org.slf4j.Logger;

import java.nio.charset.Charset;
import java.util.List;

public class DefaultEvent2LogItemHandler implements IEvent2LogItemHandler{

    private static final Logger LOG = InlongLoggerFactory.getLogger(DefaultEvent2LogItemHandler.class);

    @Override
    public List<LogItem> parse(ClsSinkContext context, ProfileEvent event) {
        String uid = event.getUid();
        ClsIdConfig idConfig = context.getIdConfig(uid);
        if (idConfig == null) {
            LOG.error("There is no cls id config for uid {}, discard it", uid);
            context.addSendResultMetric(event, context.getTaskName(), false, System.currentTimeMillis());
            return null;
        }

        String stringValues = this.getStringValues(event, idConfig);
        char delimeter = idConfig.getSeparator().charAt(0);
        List<String> listValues = UnescapeHelper.toFiledList(stringValues, delimeter);


        return null;
    }

    private String getStringValues(ProfileEvent event, ClsIdConfig idConfig) {
        byte[] bodyBytes = event.getBody();
        int msgLength = event.getBody().length;
        int contentOffset = idConfig.getContentOffset();
        if (contentOffset > 0 && msgLength >= 1) {
            return new String(bodyBytes, contentOffset, msgLength - contentOffset, Charset.defaultCharset());
        } else {
            return new String(bodyBytes, Charset.defaultCharset());
        }
    }




}
