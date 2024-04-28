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

package org.apache.inlong.manager.service.cmd.shell;

public interface ShellTracker {

    /**
     * 每次输出行发生改变时，会调用此方法
     *
     * @param runInfo shell运行信息
     */
    public void setRunInfo(String runInfo);

    /**
     * 每次输出行改变时，回调此函数
     *
     * @param line shell最新行信息
     */
    public void lineChange(String line);

    /**
     * 当执行shell文件返回进程id时调用次方法
     * 更新process进程Id
     *
     * @param processId
     */
    public void setProcessId(long processId);

    /**
     * 多线程开启之前的准备工作
     * 任务开始之前
     */
    public void beforeStart();

    /**
     * 任务开始
     */
    public void start();

    /**
     * 任务失败
     */
    public void fail(int code);

    /**
     * 任务成功
     */
    public void success();

}
