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

package org.apache.inlong.manager.dao.entity;

/**
 * CacheTopic
 */
public class CacheTopic {
    private String topicName;
    private String setName;
    private int partitionNum;

    /**
     * get topicName
     * 
     * @return the topicName
     */
    public String getTopicName() {
        return topicName;
    }

    /**
     * set topicName
     * 
     * @param topicName the topicName to set
     */
    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    /**
     * get setName
     * 
     * @return the setName
     */
    public String getSetName() {
        return setName;
    }

    /**
     * set setName
     * 
     * @param setName the setName to set
     */
    public void setSetName(String setName) {
        this.setName = setName;
    }

    /**
     * get partitionNum
     * 
     * @return the partitionNum
     */
    public int getPartitionNum() {
        return partitionNum;
    }

    /**
     * set partitionNum
     * 
     * @param partitionNum the partitionNum to set
     */
    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

}
