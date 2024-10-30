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
import org.apache.inlong.manager.service.cmd.shell.ShellExecutorImpl;
import org.apache.inlong.manager.service.cmd.shell.ShellTracker;

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

    @Override
    public CommandResult exec(String cmd) throws Exception {
        ShellTracker shellTracker = new ShellTracker();
        ShellExecutorImpl shellExecutor = new ShellExecutorImpl(shellTracker);
        shellExecutor.syncExec("sh", "-c", cmd);
        String cmdMsg = String.join(InlongConstants.BLANK, "sh", "-c", cmd);
        LOG.debug("run command : {}", cmdMsg);
        CommandResult commandResult = new CommandResult();
        commandResult.setCode(shellTracker.getCode());
        commandResult.setResult(String.join(InlongConstants.BLANK, shellTracker.getResult()));
        if (commandResult.getCode() != 0) {
            throw new Exception("command " + cmdMsg + " exec failed, code = " +
                    commandResult.getCode() + ", output = " + commandResult.getResult());
        }
        LOG.debug(commandResult.getResult());
        return commandResult;
    }

    @Override
    public CommandResult execSSHKeyGeneration() throws Exception {
        String cmdShell = "./conf/ssh_key_cmd.exp";
        ShellTracker shellTracker = new ShellTracker();
        ShellExecutorImpl shellExecutor = new ShellExecutorImpl(shellTracker);
        shellExecutor.syncExec(cmdShell);

        CommandResult commandResult = new CommandResult();
        commandResult.setCode(shellTracker.getCode());
        commandResult.setResult(String.join(InlongConstants.BLANK, shellTracker.getResult()));
        LOG.debug(commandResult.getResult());
        if (commandResult.getCode() != 0) {
            throw new Exception("SSH key generation failed, code = " +
                    commandResult.getCode() + ", output = " + commandResult.getResult());
        }
        LOG.debug(commandResult.getResult());
        return commandResult;
    }

    @Override
    public CommandResult execRemote(AgentClusterNodeRequest clusterNodeRequest, String cmd) throws Exception {
        String cmdShell = "./conf/exec_cmd.exp";
        String ip = clusterNodeRequest.getIp();
        String port = String.valueOf(clusterNodeRequest.getSshPort());
        String user = clusterNodeRequest.getUsername();
        String password = clusterNodeRequest.getPassword();
        String remoteCommandTimeout = "20000";
        // If the password is null, set the password to an empty string, and then use the public key for identification.
        if (StringUtils.isBlank(password)) {
            password = "";
        }
        cmd = "sh -c \"" + cmd + "\"";
        String cmdMsg =
                String.join(InlongConstants.BLANK, cmdShell, ip, user, password, remoteCommandTimeout, cmd, port);
        LOG.info("run remote command : {}", cmdMsg);

        ShellTracker shellTracker = new ShellTracker();
        ShellExecutorImpl shellExecutor = new ShellExecutorImpl(shellTracker);
        shellExecutor.syncExec(cmdShell, ip, user, password, remoteCommandTimeout, cmd, port);

        CommandResult commandResult = new CommandResult();
        commandResult.setCode(shellTracker.getCode());
        commandResult.setResult(String.join(InlongConstants.BLANK, shellTracker.getResult()));

        LOG.debug(commandResult.getResult());
        if (commandResult.getCode() != 0) {
            throw new Exception(
                    "remote command " + cmdMsg + " exec failed, code = " + commandResult.getCode() + ", output = "
                            + commandResult.getResult());
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
    public CommandResult tarPackage(AgentClusterNodeRequest clusterNodeRequest, String fileName, String sourcePath,
            String tarPath) throws Exception {
        String tarCmd = "tar -zxvf " + sourcePath + fileName + " -C " + tarPath;
        return execRemote(clusterNodeRequest, tarCmd);
    }

    @Override
    public CommandResult downLoadPackage(AgentClusterNodeRequest clusterNodeRequest, String downLoadPath,
            String downLoadUrl) throws Exception {
        return execRemote(clusterNodeRequest, "wget -P " + downLoadPath + InlongConstants.BLANK + downLoadUrl);
    }

    @Override
    public CommandResult mkdir(AgentClusterNodeRequest clusterNodeRequest, String path) throws Exception {
        return execRemote(clusterNodeRequest, "mkdir " + path);
    }

    @Override
    public CommandResult rmDir(AgentClusterNodeRequest clusterNodeRequest, String path) throws Exception {
        return execRemote(clusterNodeRequest, "rm -rf " + path);
    }

    @Override
    public CommandResult cpDir(AgentClusterNodeRequest clusterNodeRequest, String sourcePath, String targetPath)
            throws Exception {
        return execRemote(clusterNodeRequest, "cp " + sourcePath + " " + targetPath);
    }

}
