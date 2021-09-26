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
 * FlumeChannelExt
 */
public class FlumeChannelExt {
//    `parent_name`        varchar(128) NOT NULL COMMENT 'FlumeChannel name, English, numbers and underscore',
	private String parentName;
//    `set_name`            varchar(128) NOT NULL COMMENT 'ClusterSet name, English, numbers and underscore',
	private String setName;
//    `key_name`            varchar(64)  NOT NULL COMMENT 'Configuration item name',
	private String keyName;
//    `key_value`           varchar(256)          DEFAULT NULL COMMENT 'The value of the configuration item',
	private String keyValue;
//    `is_deleted`          tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
	private int isDeleted;

	/**
	 * get parentName
	 * 
	 * @return the parentName
	 */
	public String getParentName() {
		return parentName;
	}

	/**
	 * set parentName
	 * 
	 * @param parentName the parentName to set
	 */
	public void setParentName(String parentName) {
		this.parentName = parentName;
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
	 * get keyName
	 * 
	 * @return the keyName
	 */
	public String getKeyName() {
		return keyName;
	}

	/**
	 * set keyName
	 * 
	 * @param keyName the keyName to set
	 */
	public void setKeyName(String keyName) {
		this.keyName = keyName;
	}

	/**
	 * get keyValue
	 * 
	 * @return the keyValue
	 */
	public String getKeyValue() {
		return keyValue;
	}

	/**
	 * set keyValue
	 * 
	 * @param keyValue the keyValue to set
	 */
	public void setKeyValue(String keyValue) {
		this.keyValue = keyValue;
	}

	/**
	 * get isDeleted
	 * 
	 * @return the isDeleted
	 */
	public int getIsDeleted() {
		return isDeleted;
	}

	/**
	 * set isDeleted
	 * 
	 * @param isDeleted the isDeleted to set
	 */
	public void setIsDeleted(int isDeleted) {
		this.isDeleted = isDeleted;
	}

}
