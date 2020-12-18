/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.constants;

public class AgentConstants {

    public static final String AGENT_LOCAL_STORE_PATH = "agent.localStore.path";
    public static final String DEFAULT_AGENT_LOCAL_STORE_PATH = ".bdb";

    public static final String AGENT_DB_INSTANCE_NAME = "agent.db.instance.name";
    public static final String DEFAULT_AGENT_DB_INSTANCE_NAME = "agent";

    public static final String AGENT_DB_CLASSNAME = "agent.db.classname";
    public static final String DEFAULT_AGENT_DB_CLASSNAME = "org.apache.tubemq.agent.db.BerkeleyDB";

    // default is empty.
    public static final String AGENT_FETCHER_CLASSNAME = "agent.fetcher.classname";

    public static final String AGENT_FETCHER_INTERVAL = "agent.fetcher.interval";
    public static final int DEFAULT_AGENT_FETCHER_INTERVAL = 60;

    public static final String AGENT_CONF_PARENT = "agent.conf.parent";
    public static final String DEFAULT_AGENT_CONF_PARENT = "conf";

    public static final String AGENT_LOCAL_STORE_READONLY = "agent.localStore.readonly";
    public static final boolean DEFAULT_AGENT_LOCAL_STORE_READONLY = false;

    public static final String AGENT_HTTP_PORT = "agent.http.port";
    public static final int DEFAULT_AGENT_HTTP_PORT = 8008;

    public static final String AGENT_ENABLE_HTTP = "agent.http.enable";
    public static final boolean DEFAULT_AGENT_ENABLE_HTTP = false;

    public static final String TRIGGER_FETCH_INTERVAL = "trigger.fetch.interval";
    public static final int DEFAULT_TRIGGER_FETCH_INTERVAL = 1;

    public static final String AGENT_LOCAL_STORE_TRANSACTIONAL = "agent.localStore.transactional";
    public static final boolean DEFAULT_AGENT_LOCAL_STORE_TRANSACTIONAL = true;

    public static final String AGENT_LOCAL_STORE_LOCK_TIMEOUT = "agent.localStore.lockTimeout";
    public static final int DEFAULT_AGENT_LOCAL_STORE_LOCK_TIMEOUT = 10000;

    public static final String AGENT_LOCAL_STORE_NO_SYNC_VOID = "agent.localStore.noSyncVoid";
    public static final boolean DEFAULT_AGENT_LOCAL_STORE_NO_SYNC_VOID = false;

    public static final String AGENT_LOCAL_STORE_WRITE_NO_SYNC_VOID =
            "agent.localStore.WriteNoSyncVoid";
    public static final boolean DEFAULT_AGENT_LOCAL_STORE_WRITE_NO_SYNC_VOID = false;

    public static final String AGENT_FETCH_CENTER_INTERVAL_SECONDS = "agent.fetchCenter.interval";
    public static final int DEFAULT_AGENT_FETCH_CENTER_INTERVAL_SECONDS = 5;

    public static final String AGENT_TRIGGER_CHECK_INTERVAL_SECONDS = "agent.trigger.check.interval";
    public static final int DEFAULT_AGENT_TRIGGER_CHECK_INTERVAL_SECONDS = 1;

    public static final String THREAD_POOL_AWAIT_TIME = "thread.pool.await.time";
    public static final long DEFAULT_THREAD_POOL_AWAIT_TIME = 30;

    public static final String JOB_PENDING_MAX = "job.pending.max";
    public static final int DEFAULT_JOB_PENDING_MAX = 10;

    public static final String JOB_RUNNING_THREAD_CORE_SIZE = "job.running.thread.coreSize";
    public static final int DEFAULT_JOB_RUNNING_THREAD_CORE_SIZE = 1;

    public static final String JOB_MONITOR_INTERVAL = "job.monitor.interval";
    public static final int DEFAULT_JOB_MONITOR_INTERVAL = 5;

    public static final String JOB_RUNNING_THREAD_MAX_SIZE = "job.running.thread.maxSize";
    public static final int DEFAULT_JOB_RUNNING_THREAD_MAX_SIZE = 5;

    public static final String JOB_RUNNING_THREAD_KEEP_ALIVE = "job.running.thread.keepAlive";
    public static final long DEFAULT_JOB_RUNNING_THREAD_KEEP_ALIVE = 60L;

    public static final String JOB_FINISH_CHECK_INTERVAL = "job.finish.checkInterval";
    public static final long DEFAULT_JOB_FINISH_CHECK_INTERVAL = 6L;

    public static final String TASK_PENDING_MAX = "task.pending.max";
    public static final int DEFAULT_TASK_PENDING_MAX = 100;

    public static final String TASK_RUNNING_THREAD_CORE_SIZE = "task.running.thread.coreSize";
    public static final int DEFAULT_TASK_RUNNING_THREAD_CORE_SIZE = 4;

    public static final String TASK_RUNNING_THREAD_MAX_SIZE = "task.running.thread.maxSize";
    public static final int DEFAULT_TASK_RUNNING_THREAD_MAX_SIZE =
            Runtime.getRuntime().availableProcessors() * 2;

    public static final String TASK_RUNNING_THREAD_KEEP_ALIVE = "task.running.thread.keepAlive";
    public static final long DEFAULT_TASK_RUNNING_THREAD_KEEP_ALIVE = 60L;

    public static final String TASK_RETRY_MAX_CAPACITY = "task.retry.maxCapacity";
    public static final int DEFAULT_TASK_RETRY_MAX_CAPACITY = 10000;

    public static final String TASK_MONITOR_INTERVAL = "task.monitor.interval";
    public static final int DEFAULT_TASK_MONITOR_INTERVAL = 6;

    public static final String TASK_RETRY_SUBMIT_WAIT_SECONDS = "task.retry.submit.waitSeconds";
    public static final int DEFAULT_TASK_RETRY_SUBMIT_WAIT_SECONDS = 5;

    public static final String TASK_MAX_RETRY_TIME = "task.maxRetry.time";
    public static final int DEFAULT_TASK_MAX_RETRY_TIME = 3;

    public static final String TASK_PUSH_MAX_SECOND = "task.push.maxSecond";
    public static final int DEFAULT_TASK_PUSH_MAX_SECOND = 2;

    public static final String TASK_PULL_MAX_SECOND = "task.pull.maxSecond";
    public static final int DEFAULT_TASK_PULL_MAX_SECOND = 2;

    public static final String CHANNEL_MEMORY_CAPACITY = "channel.memory.capacity";
    public static final int DEFAULT_CHANNEL_MEMORY_CAPACITY = 5000;

    public static final String TRIGGER_CHECK_INTERVAL = "trigger.check.interval";
    public static final int DEFAULT_TRIGGER_CHECK_INTERVAL = 2;

    public static final String WORKER_POOL_AWAIT_TIME = "worker.pool.await.time";
    public static final long DEFAULT_WORKER_POOL_AWAIT_TIME = 10;
}
