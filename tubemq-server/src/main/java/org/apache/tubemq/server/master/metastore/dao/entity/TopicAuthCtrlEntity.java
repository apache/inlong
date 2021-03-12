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

package org.apache.tubemq.server.master.metastore.dao.entity;

import java.util.Date;
import org.apache.tubemq.server.common.statusdef.RuleStatus;


/*
 * store the topic authenticate control setting
 *
 */
public class TopicAuthCtrlEntity extends BaseEntity {

    private String topicName = "";
    private RuleStatus authCtrlStatus = RuleStatus.STATUS_UNDEFINE;


    public TopicAuthCtrlEntity() {
        super();
    }

    public TopicAuthCtrlEntity(String topicName, boolean enableAuth,
                               String createUser, Date createDate) {
        super(createUser, createDate);
        this.topicName = topicName;
        if (enableAuth) {
            this.authCtrlStatus = RuleStatus.STATUS_ENABLE;
        } else {
            this.authCtrlStatus = RuleStatus.STATUS_DISABLE;
        }
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public boolean isAuthCtrlEnable() {
        return authCtrlStatus == RuleStatus.STATUS_ENABLE;
    }

    public void setAuthCtrlStatus(boolean enableAuth) {
        if (enableAuth) {
            this.authCtrlStatus = RuleStatus.STATUS_ENABLE;
        } else {
            this.authCtrlStatus = RuleStatus.STATUS_DISABLE;
        }
    }
}
