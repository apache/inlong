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

package org.apache.inlong.manager.dao.entity;

import java.io.Serializable;
import java.util.Date;

import lombok.Data;

@Data
public class InlongGroupEntity implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * Incremental primary key
     */
    private Integer id;
    /**
     * Inlong group id, filled in by the user, undeleted ones cannot be repeated
     */
    private String inlongGroupId;
    /**
     * Inlong group name, English, Chinese, numbers, etc
     */
    private String name;
    /**
     * Inlong group Introduction
     */
    private String description;
    /**
     * The message queue type, high throughput: TUBE, high consistency: PULSAR
     */
    private String mqType;
    /**
     * MQ resource, for Tube, its Topic, for Pulsar, Namespace/Topic
     */
    private String mqResource;
    /**
     * Number of access records per day, unit: 10,000 records per day
     */
    private Integer dailyRecords;
    /**
     * Access size by day, unit: GB per day
     */
    private Integer dailyStorage;
    /**
     * Access peak per second, unit: records per second
     */
    private Integer peakRecords;
    /**
     * The maximum length of a single piece of data, unit: Byte
     */
    private Integer maxLength;
    /**
     * Name of responsible person, separated by commas
     */
    private String inCharges;
    /**
     * Name of followers, separated by commas
     */
    private String followers;
    /**
     * Extended params, will saved as JSON string, such as queue_module, partition_num,
     */
    private String extParams;
    /**
     * Inlong group status
     */
    private Integer status;
    /**
     * Previous group status
     */
    private Integer previousStatus;
    /**
     * Whether to delete, 0: not deleted, > 0: deleted
     */
    private Integer isDeleted;
    /**
     * Creator name
     */
    private String creator;
    /**
     * Modifier name
     */
    private String modifier;
    /**
     * Create time
     */
    private Date createTime;
    /**
     * Modify time
     */
    private Date modifyTime;
    /**
     * Need create resource, 0: false, 1: true
     */
    private Integer enableCreateResource;
    /**
     * Need zookeeper support, 0: false, 1: true
     */
    private Integer enableZookeeper;
    /**
     * The cluster tag, which links to inlong_cluster table
     */
    private String inlongClusterTag;
}