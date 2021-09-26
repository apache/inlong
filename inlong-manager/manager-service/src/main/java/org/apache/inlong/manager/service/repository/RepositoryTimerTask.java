/**
 * Tencent is pleased to support the open source community by making Tars available.
 *
 * Copyright (C) 2015,2016 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
*/

package org.apache.inlong.manager.service.repository;

import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RepositoryTimerTask
 */
public class RepositoryTimerTask<T extends IRepository> extends TimerTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryTimerTask.class);
	private T repository;

	public RepositoryTimerTask(T repository) {
		this.repository = repository;
	}

	@Override
	public void run() {
		try {
			repository.reload();
		} catch (Throwable e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
}
