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

package org.apache.inlong.agent.installer;

import org.apache.inlong.agent.common.AbstractDaemon;
import org.apache.inlong.agent.constant.AgentConstants;
import org.apache.inlong.agent.installer.conf.InstallerConfiguration;
import org.apache.inlong.agent.metrics.audit.AuditUtils;
import org.apache.inlong.agent.utils.AgentUtils;
import org.apache.inlong.agent.utils.ExcuteLinux;
import org.apache.inlong.agent.utils.HttpManager;
import org.apache.inlong.agent.utils.ThreadUtils;
import org.apache.inlong.common.pojo.agent.installer.ConfigResult;
import org.apache.inlong.common.pojo.agent.installer.ModuleConfig;
import org.apache.inlong.common.pojo.agent.installer.ModuleStateEnum;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.math.BigInteger;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_REQUEST_TIMEOUT;
import static org.apache.inlong.agent.constant.FetcherConstants.AGENT_MANAGER_VIP_HTTP_PREFIX_PATH;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_REQUEST_TIMEOUT;
import static org.apache.inlong.agent.constant.FetcherConstants.DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH;

/**
 * Installer Manager, the bridge for job manager, task manager, db e.t.c it manages agent level operations and
 * communicates with outside system.
 */
public class ModuleManager extends AbstractDaemon {

    public static final String MANAGER_ADDR = "manager.addr";
    public static final String MANAGER_AUTH_SECRET_ID = "manager.auth.secretId";
    public static final String MANAGER_AUTH_SECRET_KEY = "manager.auth.secretKey";
    public static final int CONFIG_QUEUE_CAPACITY = 1;
    public static final int CORE_THREAD_SLEEP_TIME = 10000;
    public static final int DOWNLOAD_PACKAGE_READ_BUFF_SIZE = 1024 * 1024;
    public static final String LOCAL_CONFIG_FILE = "modules.json";
    private static final Logger LOGGER = LoggerFactory.getLogger(ModuleManager.class);
    private final InstallerConfiguration conf;
    private final String confPath;
    private final BlockingQueue<ConfigResult> configQueue;
    private String currentMd5 = "";
    private Map<Integer, ModuleConfig> currentModules = new ConcurrentHashMap<>();
    private static final GsonBuilder gsonBuilder = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Gson GSON = gsonBuilder.create();
    private HttpManager httpManager;

    public ModuleManager() {
        conf = InstallerConfiguration.getInstallerConf();
        confPath = conf.get(AgentConstants.AGENT_HOME, AgentConstants.DEFAULT_AGENT_HOME) + "/conf/";
        configQueue = new LinkedBlockingQueue<>(CONFIG_QUEUE_CAPACITY);
        if (!requiredKeys(conf)) {
            throw new RuntimeException("init module manager error, cannot find required key");
        }
    }

    public HttpManager getHttpManager(InstallerConfiguration conf) {
        String managerAddr = conf.get(MANAGER_ADDR);
        String managerHttpPrefixPath = conf.get(AGENT_MANAGER_VIP_HTTP_PREFIX_PATH,
                DEFAULT_AGENT_MANAGER_VIP_HTTP_PREFIX_PATH);
        int timeout = conf.getInt(AGENT_MANAGER_REQUEST_TIMEOUT,
                DEFAULT_AGENT_MANAGER_REQUEST_TIMEOUT);
        String secretId = conf.get(MANAGER_AUTH_SECRET_ID);
        String secretKey = conf.get(MANAGER_AUTH_SECRET_KEY);
        return new HttpManager(managerAddr, managerHttpPrefixPath, timeout, secretId, secretKey);
    }

    private boolean requiredKeys(InstallerConfiguration conf) {
        return conf.hasKey(MANAGER_ADDR);
    }

    public void submitConfig(ConfigResult config) {
        if (config == null) {
            return;
        }
        configQueue.clear();
        for (int i = 0; i < config.getModuleList().size(); i++) {
            LOGGER.info("submitModules index {} total {} {}", i, config.getModuleList().size(),
                    GSON.toJson(config.getModuleList().get(i)));
        }
        configQueue.add(config);
    }

    public String getCurrentMd5() {
        return currentMd5;
    }

    public ModuleConfig getModule(Integer moduleId) {
        return currentModules.get(moduleId);
    }

    /**
     * Thread for core thread.
     *
     * @return runnable profile.
     */
    private Runnable coreThread() {
        return () -> {
            Thread.currentThread().setName("module-manager-core");
            restoreFromLocalFile(confPath);
            while (isRunnable()) {
                try {
                    dealWithConfigQueue(configQueue);
                    checkModules();
                    AuditUtils.add(AuditUtils.AUDIT_ID_AGENT_TASK_MGR_HEARTBEAT, "", "",
                            AgentUtils.getCurrentTime(), 1, 1);
                    AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME);
                } catch (Throwable ex) {
                    LOGGER.error("exception caught", ex);
                    ThreadUtils.threadThrowableHandler(Thread.currentThread(), ex);
                    AgentUtils.silenceSleepInMs(CORE_THREAD_SLEEP_TIME);
                }
            }
        };
    }

    public void restoreFromLocalFile(String confPath) {
        LOGGER.info("restore modules from local file");
        String localModuleConfigPath = confPath + LOCAL_CONFIG_FILE;
        try (Reader reader = new InputStreamReader(
                new FileInputStream(localModuleConfigPath), StandardCharsets.UTF_8)) {
            JsonElement tmpElement = JsonParser.parseReader(reader).getAsJsonObject();
            ConfigResult curConfig = GSON.fromJson(tmpElement.getAsJsonObject(), ConfigResult.class);
            if (curConfig.getMd5() != null && curConfig.getModuleList() != null) {
                currentMd5 = curConfig.getMd5();
                curConfig.getModuleList().forEach((module) -> {
                    currentModules.put(module.getId(), module);
                });
            } else {
                LOGGER.info("modules in local file invalid");
            }
        } catch (FileNotFoundException e) {
            LOGGER.info("local module json file {} not found", localModuleConfigPath);
        } catch (Exception ioe) {
            LOGGER.error("error restoredFromLocalFile {}", localModuleConfigPath, ioe);
        }
    }

    public void saveToLocalFile(String confPath) {
        File temp = new File(confPath);
        if (!temp.exists()) {
            temp.mkdirs();
        }
        File jsonPath = new File(temp.getPath() + "/" + LOCAL_CONFIG_FILE);
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(jsonPath), StandardCharsets.UTF_8))) {
            String curConfig = GSON.toJson(ConfigResult.builder().md5(currentMd5).moduleNum(currentModules.size())
                    .moduleList(currentModules.values().stream().collect(Collectors.toList())).build());
            writer.write(curConfig);
            writer.flush();
            LOGGER.info("save modules to json file");
        } catch (IOException e) {
            LOGGER.error("saveToLocalFile error: ", e);
        }
    }

    private void dealWithConfigQueue(BlockingQueue<ConfigResult> queue) {
        ConfigResult config = queue.poll();
        if (config == null) {
            return;
        }
        LOGGER.info("deal with config {}", GSON.toJson(config));
        if (currentMd5.equals(config.getMd5())) {
            LOGGER.info("md5 no change {}, skip update", currentMd5);
            return;
        }
        if (updateModules(config.getModuleList())) {
            currentMd5 = config.getMd5();
            saveToLocalFile(confPath);
        } else {
            LOGGER.error("update modules failed!");
        }
    }

    private void checkModules() {
        LOGGER.info("check modules start");
        currentModules.values().forEach((module) -> {
            LOGGER.info("check module current state {} {}", module.getName(), module.getState());
            switch (module.getState()) {
                case NEW:
                    if (downloadModule(module)) {
                        saveModuleState(module.getId(), ModuleStateEnum.DOWNLOADED);
                    } else {
                        LOGGER.error("download module {} failed, keep state in new", module.getName());
                    }
                    break;
                case DOWNLOADED:
                    if (isPackageDownloaded(module)) {
                        installModule(module);
                        saveModuleState(module.getId(), ModuleStateEnum.INSTALLED);
                    } else {
                        LOGGER.info("check module {} package failed, change stated to new, will download package again",
                                module.getName());
                        saveModuleState(module.getId(), ModuleStateEnum.NEW);
                    }
                    break;
                case INSTALLED:
                    if (!isProcessAllStarted(module)) {
                        LOGGER.info("module {} process not all started try to start", module.getName());
                        if (!startModule(module)) {
                            LOGGER.info("start module {} failed, change state to downloaded", module.getState());
                            saveModuleState(module.getId(), ModuleStateEnum.DOWNLOADED);
                        }
                    }
                    break;
                default:
                    LOGGER.error("module {} invalid state {}", module.getName(), module.getState());
            }
        });
        LOGGER.info("check modules end");
    }

    private boolean updateModules(List<ModuleConfig> managerModuleList) {
        Map<Integer, ModuleConfig> modulesFromManager = new ConcurrentHashMap<>();
        managerModuleList.forEach((moduleConfig) -> {
            modulesFromManager.put(moduleConfig.getId(), moduleConfig);
        });
        traverseManagerModulesToLocal(modulesFromManager);
        traverseLocalModulesToManager(modulesFromManager);
        return true;
    }

    private void traverseManagerModulesToLocal(Map<Integer, ModuleConfig> modulesFromManager) {
        modulesFromManager.values().forEach((managerModule) -> {
            ModuleConfig localModule = currentModules.get(managerModule.getId());
            if (localModule == null) {
                LOGGER.info("traverseManagerModulesToLocal module {} {} {} not found in local, add it",
                        managerModule.getId(), managerModule.getName(), managerModule.getVersion());
                addModule(managerModule);
            } else {
                if (managerModule.getMd5().equals(localModule.getMd5())) {
                    LOGGER.info("traverseManagerModulesToLocal module {} {} {} md5 no change, do nothing",
                            localModule.getId(), localModule.getName(), localModule.getVersion());
                } else {
                    LOGGER.info("traverseManagerModulesToLocal module {} {} {} md5 changed, update it",
                            localModule.getId(), localModule.getName(), localModule.getVersion());
                    updateModule(localModule, managerModule);
                }
            }
        });
    }

    private void traverseLocalModulesToManager(Map<Integer, ModuleConfig> modulesFromManager) {
        currentModules.values().forEach((localModule) -> {
            ModuleConfig managerModule = modulesFromManager.get(localModule.getId());
            if (managerModule == null) {
                LOGGER.info("traverseLocalModulesToManager module {} {} {} not found in local, delete it",
                        localModule.getId(), localModule.getName(), localModule.getVersion());
                deleteModule(localModule);
            }
        });
    }

    private void addModule(ModuleConfig module) {
        LOGGER.info("add module {} start", module.getName());
        addAndSaveModuleConfig(module);
        if (!downloadModule(module)) {
            LOGGER.error("add module {} but download failed", module.getName());
            return;
        }
        saveModuleState(module.getId(), ModuleStateEnum.DOWNLOADED);
        installModule(module);
        saveModuleState(module.getId(), ModuleStateEnum.INSTALLED);
        startModule(module);
        LOGGER.info("add module {} end", module.getId());
    }

    private void deleteModule(ModuleConfig module) {
        LOGGER.info("delete module {} start", module.getId());
        stopModule(module);
        uninstallModule(module);
        deleteAndSaveModuleConfig(module);
        LOGGER.info("delete module {} end", module.getId());
    }

    private void updateModule(ModuleConfig localModule, ModuleConfig managerModule) {
        LOGGER.info("update module {} start", localModule.getId());
        if (localModule.getPackageConfig().getMd5().equals(managerModule.getPackageConfig().getMd5())) {
            LOGGER.info("package md5 changed, will reinstall", localModule.getId());
            deleteModule(localModule);
            addModule(managerModule);
        } else {
            LOGGER.info("package md5 no chang, will restart", localModule.getId());
            restartModule(localModule, managerModule);
        }
        LOGGER.info("update module {} end", localModule.getId());
    }

    private void addAndSaveModuleConfig(ModuleConfig module) {
        module.setState(ModuleStateEnum.NEW);
        if (currentModules.containsKey(module.getId())) {
            LOGGER.error("should not happen! module {} found! will force to replace it!", module.getId());
        }
        currentModules.put(module.getId(), module);
        saveToLocalFile(confPath);
    }

    private void deleteAndSaveModuleConfig(ModuleConfig module) {
        if (!currentModules.containsKey(module.getId())) {
            LOGGER.error("should not happen! module {} not found!", module.getId());
            return;
        }
        currentModules.remove(module.getId());
        saveToLocalFile(confPath);
    }

    private boolean saveModuleState(Integer moduleId, ModuleStateEnum state) {
        ModuleConfig module = currentModules.get(moduleId);
        if (module == null) {
            LOGGER.error("should not happen! module {} not found!", moduleId);
            return false;
        }
        module.setState(state);
        saveToLocalFile(confPath);
        LOGGER.info("save module state to {} {}", moduleId, state);
        return true;
    }

    private void restartModule(ModuleConfig localModule, ModuleConfig managerModule) {
        stopModule(localModule);
        startModule(managerModule);
    }

    private void installModule(ModuleConfig module) {
        LOGGER.info("install module {} with cmd {}", module.getId(), module.getInstallCommand());
        String ret = ExcuteLinux.exeCmd(module.getInstallCommand());
        LOGGER.info("install module {} return {} ", module.getId(), ret);
    }

    private boolean startModule(ModuleConfig module) {
        LOGGER.info("start module {} with cmd {}", module.getId(), module.getStartCommand());
        for (int i = 0; i < module.getProcessesNum(); i++) {
            String ret = ExcuteLinux.exeCmd(module.getStartCommand());
            LOGGER.info("start [{}] module {} return {} ", i, module.getId(), ret);
        }
        if (isProcessAllStarted(module)) {
            LOGGER.info("start module {} success", module.getId());
            return true;
        } else {
            LOGGER.info("start module {} failed", module.getId());
            return false;
        }
    }

    private void stopModule(ModuleConfig module) {
        LOGGER.info("stop module {} with cmd {}", module.getId(), module.getStopCommand());
        String ret = ExcuteLinux.exeCmd(module.getStopCommand());
        LOGGER.info("stop module {} return {} ", module.getId(), ret);
    }

    private void uninstallModule(ModuleConfig module) {
        LOGGER.info("uninstall module {} with cmd {}", module.getId(), module.getUninstallCommand());
        String ret = ExcuteLinux.exeCmd(module.getUninstallCommand());
        LOGGER.info("uninstall module {} return {} ", module.getId(), ret);
    }

    private boolean isProcessAllStarted(ModuleConfig module) {
        String ret = ExcuteLinux.exeCmd(module.getCheckCommand());
        String[] processArray = ret.split("\n");
        int cnt = 0;
        for (int i = 0; i < processArray.length; i++) {
            if (processArray[i].length() > 0) {
                cnt++;
            }
        }
        LOGGER.info("get module process num {} {}", module.getName(), cnt);
        return cnt >= module.getProcessesNum();
    }

    private boolean downloadModule(ModuleConfig module) {
        LOGGER.info("download module {} begin with url {}", module.getId(), module.getPackageConfig().getDownloadUrl());
        try {
            URL url = new URL(module.getPackageConfig().getDownloadUrl());
            URLConnection conn = url.openConnection();
            Map<String, String> authHeader = httpManager.getAuthHeader();
            authHeader.forEach((k, v) -> {
                conn.setRequestProperty(k, v);
            });
            String path = module.getPackageConfig().getStoragePath() + "/" + module.getPackageConfig().getFileName();
            try (InputStream inputStream = conn.getInputStream();
                    FileOutputStream outputStream = new FileOutputStream(path)) {
                LOGGER.info("save path {}", path);
                int byteRead;
                byte[] buffer = new byte[DOWNLOAD_PACKAGE_READ_BUFF_SIZE];
                while ((byteRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, byteRead);
                }
            }
            if (isPackageDownloaded(module)) {
                return true;
            } else {
                LOGGER.error("download package md5 not match!");
                return false;
            }
        } catch (FileNotFoundException e) {
            LOGGER.error("download module err", e);
        } catch (IOException e) {
            LOGGER.error("download module err", e);
        }
        LOGGER.info("download module {} end", module.getId());
        return false;
    }

    private boolean isPackageDownloaded(ModuleConfig module) {
        String path = module.getPackageConfig().getStoragePath() + "/" + module.getPackageConfig().getFileName();
        if (calcFileMd5(path).equals(module.getPackageConfig().getMd5())) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void start() throws Exception {
        httpManager = getHttpManager(conf);
        submitWorker(coreThread());
    }

    @Override
    public void join() {
        super.join();
    }

    /**
     * It should guarantee thread-safe, and can be invoked many times.
     *
     * @throws Exception exceptions
     */
    @Override
    public void stop() throws Exception {
        waitForTerminate();
    }

    private static String calcFileMd5(String path) {
        BigInteger bi = null;
        byte[] buffer = new byte[DOWNLOAD_PACKAGE_READ_BUFF_SIZE];
        int len = 0;
        try (FileInputStream fileInputStream = new FileInputStream(path)) {
            MessageDigest md = MessageDigest.getInstance("MD5");
            while ((len = fileInputStream.read(buffer)) != -1) {
                md.update(buffer, 0, len);
            }
            byte[] b = md.digest();
            bi = new BigInteger(1, b);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("calc file md5 NoSuchAlgorithmException", e);
            return "";
        } catch (IOException e) {
            LOGGER.error("calc file md5 IOException", e);
            return "";
        }
        return bi.toString(16);
    }
}
