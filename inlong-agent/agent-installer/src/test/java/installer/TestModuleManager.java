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

package installer;

import org.apache.inlong.agent.installer.ModuleManager;
import org.apache.inlong.common.pojo.agent.installer.ConfigResult;
import org.apache.inlong.common.pojo.agent.installer.ModuleConfig;
import org.apache.inlong.common.pojo.agent.installer.ModuleStateEnum;
import org.apache.inlong.common.pojo.agent.installer.PackageConfig;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ModuleManager.class)
@PowerMockIgnore({"javax.management.*"})
public class TestModuleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestModuleManager.class);
    private static BaseTestsHelper helper;
    private static final ClassLoader LOADER = TestModuleManager.class.getClassLoader();
    private static ModuleManager manager;
    private static List<Action> realActionList = new ArrayList<>();
    private static List<Action> expectedActionList = new ArrayList<>();
    private static String OLD_MD5 = "95648c83b45971dce503d5d844496cfc";
    private static String NEW_MD5 = "e573f399da60ddeff09904bb95bdc307";

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Action {

        public ModuleActionTypeEnum type;
        public String md5;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Action action = (Action) o;
            return Objects.equals(type, action.type) &&
                    Objects.equals(md5, action.md5);
        }
    }

    @BeforeClass
    public static void setup() {
        helper = new BaseTestsHelper(TestModuleManager.class.getName()).setupAgentHome();
        manager = PowerMockito.spy(new ModuleManager());
    }

    @AfterClass
    public static void teardown() throws Exception {
        manager.stop();
    }

    private void fillExpectedActionList() {
        expectedActionList.add(new Action(ModuleActionTypeEnum.STOP, OLD_MD5));
        expectedActionList.add(new Action(ModuleActionTypeEnum.UNINSTALL, OLD_MD5));
        expectedActionList.add(new Action(ModuleActionTypeEnum.DOWNLOAD, NEW_MD5));
        expectedActionList.add(new Action(ModuleActionTypeEnum.INSTALL, NEW_MD5));
        expectedActionList.add(new Action(ModuleActionTypeEnum.START, NEW_MD5));
    }

    private void mockFunctions() {
        try {
            PowerMockito.doAnswer(invocation -> {
                ModuleConfig module = invocation.getArgument(0);
                realActionList.add(new Action(ModuleActionTypeEnum.DOWNLOAD, module.getPackageConfig().getMd5()));
                return true;
            }).when(manager, "downloadModule", Mockito.any());

            PowerMockito.doAnswer(invocation -> {
                ModuleConfig module = invocation.getArgument(0);
                realActionList.add(new Action(ModuleActionTypeEnum.INSTALL, module.getPackageConfig().getMd5()));
                return true;
            }).when(manager, "installModule", Mockito.any());

            PowerMockito.doAnswer(invocation -> {
                ModuleConfig module = invocation.getArgument(0);
                realActionList.add(new Action(ModuleActionTypeEnum.UNINSTALL, module.getPackageConfig().getMd5()));
                return true;
            }).when(manager, "uninstallModule", Mockito.any());

            PowerMockito.doAnswer(invocation -> {
                ModuleConfig module = invocation.getArgument(0);
                realActionList.add(new Action(ModuleActionTypeEnum.START, module.getPackageConfig().getMd5()));
                return true;
            }).when(manager, "startModule", Mockito.any());

            PowerMockito.doAnswer(invocation -> {
                ModuleConfig module = invocation.getArgument(0);
                realActionList.add(new Action(ModuleActionTypeEnum.STOP, module.getPackageConfig().getMd5()));
                return true;
            }).when(manager, "stopModule", Mockito.any());

            PowerMockito.doAnswer(invocation -> {
                ModuleConfig module = invocation.getArgument(0);
                return true;
            }).when(manager, "isProcessAllStarted", Mockito.any());

            PowerMockito.doReturn(null).when(manager, "getHttpManager", Mockito.any());
        } catch (Exception e) {
            LOGGER.error("mock downloadModule error", e);
            Assert.fail();
        }
    }

    @Test
    public void testModuleManager() {
        fillExpectedActionList();
        mockFunctions();
        String confPath = LOADER.getResource("conf/").getPath();
        manager.restoreFromLocalFile(confPath);
        Assert.assertEquals(manager.getModule(1).getPackageConfig().getMd5(), OLD_MD5);
        manager.submitConfig(getConfig());
        try {
            manager.start();
        } catch (Exception e) {
            LOGGER.error("start module manager error", e);
            Assert.fail("start module manager error");
        }
        await().atMost(3, TimeUnit.SECONDS).until(() -> manager.getModule(1) != null);
        await().atMost(10, TimeUnit.SECONDS).until(() -> realActionList.size() == expectedActionList.size());
        for (int i = 0; i < expectedActionList.size(); i++) {
            LOGGER.info("{} {}", realActionList.get(i), expectedActionList.get(i));
            Assert.assertEquals(Integer.toString(i), realActionList.get(i), expectedActionList.get(i));
        }
    }

    private ConfigResult getConfig() {
        List<ModuleConfig> configs = new ArrayList<>();
        configs.add(getModuleConfig(1, "inlong-agent", "inlong-agent-md5-185454", "1.0", 1,
                "cd ~/inlong-agent/bin;sh agent.sh start", "cd ~/inlong-agent/bin;sh agent.sh stop",
                "ps aux | grep core.AgentMain | grep java | grep -v grep | awk '{print $2}'",
                "cd ~/inlong-agent/bin;sh agent.sh stop;rm -rf ~/inlong-agent/;mkdir ~/inlong-agent;cd /tmp;tar -xzvf agent-release-1.13.0-SNAPSHOT-bin.tar.gz -C ~/inlong-agent;cd ~/inlong-agent/bin;sh agent.sh start",
                "echo empty uninstall cmd", "agent-release-1.13.0-SNAPSHOT-bin.tar.gz",
                "http://11.151.252.111:8083/inlong/manager/openapi/agent/download/agent-release-1.13.0-SNAPSHOT-bin.tar.gz",
                NEW_MD5));
        return ConfigResult.builder().moduleList(configs).md5("config-result-md5-193603").version(1).build();
    }

    private ModuleConfig getModuleConfig(int id, String name, String md5, String version, Integer procNum,
            String startCmd, String stopCmd, String checkCmd, String installCmd, String uninstallCmd, String fileName,
            String downloadUrl,
            String packageMd5) {
        ModuleConfig moduleConfig = new ModuleConfig();
        moduleConfig.setId(id);
        moduleConfig.setName(name);
        moduleConfig.setVersion(version);
        moduleConfig.setMd5(md5);
        moduleConfig.setProcessesNum(procNum);
        moduleConfig.setStartCommand(startCmd);
        moduleConfig.setStopCommand(stopCmd);
        moduleConfig.setCheckCommand(checkCmd);
        moduleConfig.setInstallCommand(installCmd);
        moduleConfig.setUninstallCommand(uninstallCmd);
        PackageConfig packageConfig = new PackageConfig();
        packageConfig.setFileName(fileName);
        packageConfig.setDownloadUrl(downloadUrl);
        packageConfig.setStoragePath("/tmp");
        packageConfig.setMd5(packageMd5);
        moduleConfig.setPackageConfig(packageConfig);
        moduleConfig.setState(ModuleStateEnum.NEW);
        return moduleConfig;
    }
}
