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
 * FlumeSource
 */
public class FlumeSource {
    private String sourceName;
    private String setName;
    private String type;
    private String channels;
    private String selectorType;

    /**
     * get sourceName
     * 
     * @return the sourceName
     */
    public String getSourceName() {
        return sourceName;
    }

    /**
     * set sourceName
     * 
     * @param sourceName the sourceName to set
     */
    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
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
     * get type
     * 
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * set type
     * 
     * @param type the type to set
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * get channels
     * 
     * @return the channels
     */
    public String getChannels() {
        return channels;
    }

    /**
     * set channels
     * 
     * @param channels the channels to set
     */
    public void setChannels(String channels) {
        this.channels = channels;
    }

    /**
     * get selectorType
     * 
     * @return the selectorType
     */
    public String getSelectorType() {
        return selectorType;
    }

    /**
     * set selectorType
     * 
     * @param selectorType the selectorType to set
     */
    public void setSelectorType(String selectorType) {
        this.selectorType = selectorType;
    }

}
