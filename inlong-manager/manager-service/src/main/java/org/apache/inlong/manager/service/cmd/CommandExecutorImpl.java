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

package org.apache.inlong.manager.service.cmd;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.pojo.cluster.agent.AgentClusterNodeRequest;
import org.apache.inlong.manager.service.cmd.shell.ShellExecutor;
import org.apache.inlong.manager.service.cmd.shell.SimpleTracker;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class CommandExecutorImpl implements CommandExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(CommandExecutorImpl.class);

    private String join(List<String> strings) {
        StringBuilder buffer = new StringBuilder();
        for (String string : strings) {
            buffer.append(string).append(InlongConstants.NEW_LINE);
        }
        return buffer.toString();
    }

    private String join(String... strings) {
        StringBuffer buffer = new StringBuffer();
        for (String string : strings) {
            buffer.append(string).append(InlongConstants.BLANK);
        }
        return buffer.toString();
    }

    @Override
    public CommandResult exec(String cmd) throws Exception {
        SimpleTracker shellTracker = new SimpleTracker();
        ShellExecutor shellExecutor = new ShellExecutor(shellTracker);
        shellExecutor.syncExec("sh", "-c", cmd);
        String cmdMsg = join("sh", "-c", cmd);
        LOG.debug("run command : " + cmdMsg);
        CommandResult commandResult = new CommandResult();
        commandResult.setCode(shellTracker.getCode());
        commandResult.setStdout(join(shellTracker.getResult()));
        if (commandResult.getCode() != 0) {
            throw new Exception("command " + cmdMsg + " exec failed, code = " +
                    commandResult.getCode() + ", output = " + commandResult.getStdout());
        }
        LOG.debug(commandResult.getStdout());
        return commandResult;
    }

    @Override
    public CommandResult execRemote(AgentClusterNodeRequest clusterNodeRequest, String cmd) throws Exception {
        String cmdShell = "./conf/exec_remote_cmd.exp";
        String ip = clusterNodeRequest.getIp();
        String port = String.valueOf(clusterNodeRequest.getPort());
        String user = clusterNodeRequest.getUsername();
        String password = clusterNodeRequest.getPassword();
        String remoteCommandTimeout = "20000";

        cmd = "sh -c \"" + cmd + "\"";
        String cmdMsg = join(cmdShell, ip, user, password, remoteCommandTimeout, cmd, port);
        LOG.info("run remote command : " + cmdMsg);

        SimpleTracker shellTracker = new SimpleTracker();
        ShellExecutor shellExecutor = new ShellExecutor(shellTracker);
        shellExecutor.syncExec(cmdShell, ip, user, password, remoteCommandTimeout, cmd, port);

        CommandResult commandResult = new CommandResult();
        commandResult.setCode(shellTracker.getCode());
        commandResult.setStdout(join(shellTracker.getResult()));

        LOG.debug(commandResult.getStdout());
        if (commandResult.getCode() != 0) {
            throw new Exception(
                    "remote command " + cmdMsg + " exec failed, code = " + commandResult.getCode() + ", output = "
                            + commandResult.getStdout());
        }
        return commandResult;
    }

    @Override
    public CommandResult modifyConfig(AgentClusterNodeRequest clusterNodeRequest, Map<String, String> configMap,
            String confPath)
            throws Exception {
        List<String> configList = configMap.entrySet().stream()
                .map(entry -> "grep " + entry.getKey() + " " + confPath + " && sed -i 's%^" + entry.getKey() + ".*%"
                        + entry.getKey() + InlongConstants.EQUAL + entry.getValue() + "%' " + confPath
                        + " || echo " + entry.getKey() + InlongConstants.EQUAL + entry.getValue() + " >> "
                        + confPath)
                .collect(Collectors.toList());
        String modifyCmd = StringUtils.join(configList, ";");
        return this.execRemote(clusterNodeRequest, modifyCmd);
    }

    @Override
    public CommandResult tarPackage(AgentClusterNodeRequest clusterNodeRequest, String fileName,
            String tarPath) throws Exception {
        String tarCmd = "tar -zxvf " + tarPath + fileName + " -C " + tarPath;
        return execRemote(clusterNodeRequest, tarCmd);
    }

    @Override
    public CommandResult downLoadPackage(AgentClusterNodeRequest clusterNodeRequest, String downLoadPath,
            String downLoadUrl) throws Exception {
        return execRemote(clusterNodeRequest, "wget -P " + downLoadPath + downLoadUrl);
    }

    @Override
    public CommandResult mkdir(AgentClusterNodeRequest clusterNodeRequest, String path) throws Exception {
        return execRemote(clusterNodeRequest, "mkdir " + path);
    }

}
