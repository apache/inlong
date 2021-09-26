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
 * CacheClusterExt
 */
public class CacheClusterExt {
//    `cluster_name`        varchar(128) NOT NULL COMMENT 'CacheCluster name, English, numbers and underscore',
	private String clusterName;
//    `key_name`            varchar(64)  NOT NULL COMMENT 'Configuration item name',
	private String keyName;
//    `key_value`           varchar(256)          DEFAULT NULL COMMENT 'The value of the configuration item',
	private String keyValue;
//    `is_deleted`          tinyint(1)            DEFAULT '0' COMMENT 'Whether to delete, 0: not deleted, 1: deleted',
	private int isDeleted;

	/**
	 * get clusterName
	 * 
	 * @return the clusterName
	 */
	public String getClusterName() {
		return clusterName;
	}

	/**
	 * set clusterName
	 * 
	 * @param clusterName the clusterName to set
	 */
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
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
