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
package org.apache.inlong.manager.common.pojo.dataproxy;

/**
 * FlumeChannel
 */
public class FlumeChannel {
//    `channel_name`        varchar(128) NOT NULL COMMENT 'FlumeChannel name, English, numbers and underscore',
	private String channelName;
//    `set_name`            varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
	private String setName;
//    `type`                varchar(128)  NOT NULL COMMENT 'FlumeChannel classname',
	private String type;

	/**
	 * get channelName
	 * 
	 * @return the channelName
	 */
	public String getChannelName() {
		return channelName;
	}

	/**
	 * set channelName
	 * 
	 * @param channelName the channelName to set
	 */
	public void setChannelName(String channelName) {
		this.channelName = channelName;
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

}
