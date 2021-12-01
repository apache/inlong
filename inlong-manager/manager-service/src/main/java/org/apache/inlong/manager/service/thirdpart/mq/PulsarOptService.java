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

package org.apache.inlong.manager.service.thirdpart.mq;

import java.util.List;
import org.apache.inlong.manager.common.pojo.pulsar.PulsarTopicBean;
import org.apache.inlong.manager.dao.entity.BusinessPulsarEntity;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

/**
 * Interface of Pulsar operation
 */
public interface PulsarOptService {

    void createTenant(PulsarAdmin pulsarAdmin, String tenant) throws PulsarAdminException;

    void createNamespace(PulsarAdmin pulsarAdmin, BusinessPulsarEntity pulsarEntity, String tenant,
            String namespace) throws PulsarAdminException;

    void createTopic(PulsarAdmin pulsarAdmin, PulsarTopicBean topicBean) throws PulsarAdminException;

    void createSubscription(PulsarAdmin pulsarAdmin, PulsarTopicBean topicBean, String subscription)
            throws PulsarAdminException;

    void createSubscriptions(PulsarAdmin pulsarAdmin, String subscription, PulsarTopicBean topicBean,
            List<String> topics) throws PulsarAdminException;

    boolean topicIsExists(PulsarAdmin pulsarAdmin, String tenant, String namespace, String topic)
            throws PulsarAdminException;

}
