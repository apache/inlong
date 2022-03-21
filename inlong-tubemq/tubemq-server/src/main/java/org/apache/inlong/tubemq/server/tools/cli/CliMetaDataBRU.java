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

package org.apache.inlong.tubemq.server.tools.cli;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.inlong.tubemq.corebase.cluster.MasterInfo;
import org.apache.inlong.tubemq.corebase.rv.ProcessResult;
import org.apache.inlong.tubemq.corebase.utils.DateTimeConvertUtils;
import org.apache.inlong.tubemq.corebase.utils.TStringUtils;
import org.apache.inlong.tubemq.server.common.TServerConstants;
import org.apache.inlong.tubemq.server.common.fielddef.CliArgDef;
import org.apache.inlong.tubemq.server.common.statusdef.ManageStatus;
import org.apache.inlong.tubemq.server.common.statusdef.TopicStatus;
import org.apache.inlong.tubemq.server.common.utils.HttpUtils;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.BaseEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.BrokerConfEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.ClusterSettingEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupConsumeCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.GroupResCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicCtrlEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicDeployEntity;
import org.apache.inlong.tubemq.server.master.metamanage.metastore.dao.entity.TopicPropGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CliMetaDataBRU, metadata backup and recovery utility.
 * The utility class for script #{bin/tubemq-metadata-bru.sh} to
 * backup and recovery metadata from Masters.
 *
 */
public class CliMetaDataBRU extends CliAbstractBase {

    private static final Logger logger = LoggerFactory.getLogger(CliMetaDataBRU.class);
    private static final List<String> allowedOpTypeList = Arrays.asList("backup", "recovery");
    private static final int maxDataLength = 150000;
    private static final String curOperator = "SystemCliAdmin";
    // cli parameters
    private String masterServers = null;
    private String operationType = null;
    private String authToken = null;
    private String backupAndRecoveryPath = "./meta_backup";
    private File metaDataDir = null;
    private MasterInfo masterInfo;

    public CliMetaDataBRU() {
        super("tubemq-metadata-bru.sh");
        initCommandOptions();
    }

    /**
     * Init command options
     */
    @Override
    protected void initCommandOptions() {
        // add the cli required parameters
        addCommandOption(CliArgDef.MASTERSERVER);
        addCommandOption(CliArgDef.OPERATIONTYPE);
        addCommandOption(CliArgDef.METAFILEPATH);
        addCommandOption(CliArgDef.AUTHTOKEN);
    }

    @Override
    public boolean processParams(String[] args) throws Exception {
        // parse parameters and check value
        CommandLine cli = parser.parse(options, args);
        if (cli == null) {
            throw new org.apache.commons.cli.ParseException("Parse args failure");
        }
        if (cli.hasOption(CliArgDef.VERSION.longOpt)) {
            version();
        }
        if (cli.hasOption(CliArgDef.HELP.longOpt)) {
            help();
        }
        // get master-addresses
        if (!cli.hasOption(CliArgDef.MASTERSERVER.longOpt)) {
            throw new Exception(CliArgDef.MASTERSERVER.longOpt + " is required!");
        }
        masterServers = cli.getOptionValue(CliArgDef.MASTERSERVER.longOpt);
        if (TStringUtils.isBlank(masterServers)) {
            throw new Exception(CliArgDef.MASTERSERVER.longOpt + " is not allowed blank!");
        }
        masterInfo =  new MasterInfo(masterServers.trim());
        // get operation-type
        if (!cli.hasOption(CliArgDef.OPERATIONTYPE.longOpt)) {
            throw new Exception(CliArgDef.OPERATIONTYPE.longOpt + " is required!");
        }
        operationType = cli.getOptionValue(CliArgDef.OPERATIONTYPE.longOpt);
        if (TStringUtils.isBlank(operationType)) {
            throw new Exception(CliArgDef.OPERATIONTYPE.longOpt + " is not allowed blank!");
        }
        if (!allowedOpTypeList.contains(operationType)) {
            throw new Exception(CliArgDef.OPERATIONTYPE.longOpt
                    + " only supports " + allowedOpTypeList.toString());
        }
        // get metadata backup and recovery path
        if (cli.hasOption(CliArgDef.METAFILEPATH.longOpt)) {
            backupAndRecoveryPath = cli.getOptionValue(CliArgDef.METAFILEPATH.longOpt);
            if (TStringUtils.isBlank(backupAndRecoveryPath)) {
                throw new Exception(CliArgDef.METAFILEPATH.longOpt + " is not allowed blank!");
            }
        }
        // validate path directory
        metaDataDir = new File(backupAndRecoveryPath);
        if (!metaDataDir.exists()) {
            if (operationType.equalsIgnoreCase("backup")) {
                if (!metaDataDir.mkdirs()) {
                    throw new IOException(new StringBuilder(512)
                            .append("Creates the directory named ")
                            .append(metaDataDir.getAbsolutePath())
                            .append(" failure!").toString());
                }
            } else {
                throw new RuntimeException(new StringBuilder(512)
                        .append("Path ").append(backupAndRecoveryPath)
                        .append(" is not existed!").toString());
            }
        }
        if (!metaDataDir.isDirectory() || !metaDataDir.canRead()) {
            throw new RuntimeException(new StringBuilder(512)
                    .append("Path ").append(backupAndRecoveryPath)
                    .append(" is not a readable directory!").toString());
        }
        // get auth-token code
        if (operationType.equalsIgnoreCase("recovery")) {
            authToken = cli.getOptionValue(CliArgDef.AUTHTOKEN.longOpt);
            if (TStringUtils.isBlank(authToken)) {
                throw new Exception(CliArgDef.AUTHTOKEN.longOpt + " is not allowed blank!");
            }
        }
        return true;
    }

    public static void main(final String[] args) {
        CliMetaDataBRU cliMetaDataBRU = new CliMetaDataBRU();
        StringBuilder strBuff = new StringBuilder(512);
        try {
            boolean result = cliMetaDataBRU.processParams(args);
            if (!result) {
                throw new Exception("Parse parameters failure!");
            }
            cliMetaDataBRU.processCommands(strBuff);
        } catch (Throwable ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
            cliMetaDataBRU.help();
        }
    }

    public void processCommands(StringBuilder strBuff) {
        if (operationType.equals("backup")) {
            backupMetaData(strBuff);
        } else {
            recoveryMetaData(strBuff);
        }
    }

    /**
     * Backup meta data from Masters
     * The Master currently has 6 types of metadata.
     * First, query the metadata from the Master, then save it to the specified storage location,
     * and finally read the saved result and compare it with the queried data to confirm that
     * the saved content is consistent with the query result.
     *
     * @param strBuff  the string buffer
     */
    private void backupMetaData(StringBuilder strBuff) {
        logger.info("Backup meta-data begin: start query data from remote, total 6 items");
        // a. cluster setting
        logger.info("No 1.1-6: download cluster setting configurations from Master");
        Map<String, ClusterSettingEntity> clusterSettingMap = getClusterConfInfo(strBuff);
        if (clusterSettingMap == null) {
            logger.error("    download cluster setting configurations failure!");
            return;
        }
        logger.info("No 1.2-6: store cluster setting configurations to local file");
        storeObjectToFile(clusterSettingMap, backupAndRecoveryPath, "clusterConfig");
        logger.info("No 1.3-6: verify configurations ");
        Map<String, ClusterSettingEntity> storedSettingMap =
                (Map<String, ClusterSettingEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "clusterConfig");
        if (storedSettingMap == null) {
            logger.error(strBuff
                    .append("    local cluster setting configurations read failure!, clusterConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        if (storedSettingMap.size() != clusterSettingMap.size()) {
            logger.error("    verify failure, stored clusterConfig size not equal!");
            return;
        }
        for (Map.Entry<String, ClusterSettingEntity> qryEntry : clusterSettingMap.entrySet()) {
            ClusterSettingEntity targetEntity = storedSettingMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored clusterConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                strBuff.delete(0, strBuff.length());
                return;
            }
        }
        // b. broker configurations
        logger.info("No 2.1-6: download broker configurations from Master");
        Map<String, BrokerConfEntity> brokerConfMap = getBrokerConfInfos(strBuff);
        if (brokerConfMap == null) {
            logger.error("    download broker configurations is null!");
            return;
        }
        logger.info("No 2.2-6: store broker configurations to local file");
        storeObjectToFile(brokerConfMap, backupAndRecoveryPath, "brokerConfig");
        logger.info("No 2.3-6: verify configurations");
        Map<String, BrokerConfEntity> storedBrokerConfigMap =
                (Map<String, BrokerConfEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "brokerConfig");
        if (storedBrokerConfigMap == null) {
            logger.error(strBuff
                    .append("    local broker configurations read failure!, brokerConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        if (storedBrokerConfigMap.size() != brokerConfMap.size()) {
            logger.error("    verify failure, stored brokerConfig size not equal!");
            return;
        }
        for (Map.Entry<String, BrokerConfEntity> qryEntry : brokerConfMap.entrySet()) {
            BrokerConfEntity targetEntity = storedBrokerConfigMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored brokerConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                return;
            }
        }
        // c. topic control configurations
        logger.info("No 3.1-6: download topic control configurations from Master");
        Map<String, TopicCtrlEntity> topicCtrlMap = getTopicControlInfos(strBuff);
        if (topicCtrlMap == null) {
            logger.error("    download topic control configure is null!");
            return;
        }
        logger.info("No 3.2-6: store topic control configurations to local file");
        storeObjectToFile(topicCtrlMap, backupAndRecoveryPath, "topicControlConfig");
        logger.info("No 3.3-6: verify configurations");
        Map<String, TopicCtrlEntity> storedTopicCtrlMap =
                (Map<String, TopicCtrlEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "topicControlConfig");
        if (storedTopicCtrlMap == null) {
            logger.error(strBuff
                    .append("    local topic control configurations read failure!, topicControlConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        if (storedTopicCtrlMap.size() != topicCtrlMap.size()) {
            logger.error("    verify failure, stored topicControlConfig size not equal!");
            return;
        }
        for (Map.Entry<String, TopicCtrlEntity> qryEntry : topicCtrlMap.entrySet()) {
            TopicCtrlEntity targetEntity = storedTopicCtrlMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored topicControlConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                return;
            }
        }
        // d. topic deploy configurations
        logger.info("No 4.1-6: download topic deploy configurations from Master");
        Map<String, TopicDeployEntity> topicDeployMap = getTopicDeployInfos(strBuff);
        if (topicDeployMap == null) {
            logger.error("    download topic deploy configurations is null!");
            return;
        }
        logger.info("No 4.2-6: store topic deploy configurations to local file");
        storeObjectToFile(topicDeployMap, backupAndRecoveryPath, "topicDeployConfig");
        logger.info("No 4.3-6: verify configurations");
        Map<String, TopicDeployEntity> storedTopicDeployMap =
                (Map<String, TopicDeployEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "topicDeployConfig");
        if (storedTopicDeployMap == null) {
            logger.error(strBuff
                    .append("    local topic deploy configurations read failure!, topicControlConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        if (storedTopicDeployMap.size() != topicDeployMap.size()) {
            logger.error("    verify failure, stored topicDeployConfig size not equal!");
            return;
        }
        for (Map.Entry<String, TopicDeployEntity> qryEntry : topicDeployMap.entrySet()) {
            TopicDeployEntity targetEntity = storedTopicDeployMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored topicDeployConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                return;
            }
        }
        // e. group control configurations
        logger.info("No 5.1-6: download group control configurations from Master");
        Map<String, GroupResCtrlEntity> groupCtrlMap = getGroupResCtrlInfos(strBuff);
        if (groupCtrlMap == null) {
            logger.error("    download group control configurations is null!");
            return;
        }
        logger.info("No 5.2-6: store group control configurations to local file");
        storeObjectToFile(groupCtrlMap, backupAndRecoveryPath, "groupCtrlConfig");
        logger.info("No 5.3-6: verify configurations");
        Map<String, GroupResCtrlEntity> storedGroupCtrlMap =
                (Map<String, GroupResCtrlEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "groupCtrlConfig");
        if (storedGroupCtrlMap == null) {
            logger.error(strBuff
                    .append("    local group control configurations read failure!, groupCtrlConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        if (storedGroupCtrlMap.size() != groupCtrlMap.size()) {
            logger.error("    verify failure, stored groupCtrlConfig size not equal!");
            return;
        }
        for (Map.Entry<String, GroupResCtrlEntity> qryEntry : groupCtrlMap.entrySet()) {
            GroupResCtrlEntity targetEntity = storedGroupCtrlMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored groupCtrlConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                return;
            }
        }
        // f. group consume control configurations
        logger.info("No 6.1-6: download group consume configurations from Master");
        Map<String, GroupConsumeCtrlEntity> groupCsmInfoMap = getGroupCsmCtrlInfos(strBuff);
        if (groupCsmInfoMap == null) {
            logger.error("    download group consume configurations is null!");
            return;
        }
        logger.info("No 6.2-6: store group consume configurations to local file");
        storeObjectToFile(groupCsmInfoMap, backupAndRecoveryPath, "groupConsumeConfig");
        logger.info("No 6.3-6: verify configurations");
        Map<String, GroupConsumeCtrlEntity> storedGroupCsmMap =
                (Map<String, GroupConsumeCtrlEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "groupConsumeConfig");
        if (storedGroupCsmMap == null) {
            logger.error(strBuff
                    .append("    local group consume configurations read failure!, groupConsumeConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        if (storedGroupCsmMap.size() != groupCsmInfoMap.size()) {
            logger.error("    verify failure, stored groupConsumeConfig size not equal!");
            return;
        }
        for (Map.Entry<String, GroupConsumeCtrlEntity> qryEntry : groupCsmInfoMap.entrySet()) {
            GroupConsumeCtrlEntity targetEntity = storedGroupCsmMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored groupConsumeConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                return;
            }
        }
        logger.info("Backup finished!");
    }

    /**
     * Recovery meta data to Masters
     * For each type of metadata, they follow the same set of processing procedures:
     * read metadata locally, write the data to the Master, read the written data from the Master,
     * and then compare the data read from the Master and the  locally stored data,
     * determine whether the two are consistent
     *
     * @param strBuff  the string buffer
     */
    private void recoveryMetaData(StringBuilder strBuff) {
        logger.info("Recovery meta-data begin: start read data from local path");
        // a. read cluster configurations
        logger.info("No 1.1-6  recovery cluster configurations");
        Map<String, ClusterSettingEntity> storedSettingMap =
                (Map<String, ClusterSettingEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "clusterConfig");
        if (storedSettingMap == null) {
            logger.error(strBuff
                    .append("    local cluster setting configurations read failure!, clusterConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 1.2-6  upload cluster configurations to master");
        ProcessResult result = new ProcessResult();
        if (!writeClusterConfInfo(storedSettingMap, strBuff, result)) {
            logger.error(strBuff.append("    write cluster configurations failure!")
                    .append(result.getErrMsg()).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 1.3-6  read restored cluster configurations from master");
        Map<String, ClusterSettingEntity> clusterSettingMap = getClusterConfInfo(strBuff);
        if (clusterSettingMap == null) {
            logger.error("    read restored cluster setting configurations failure!");
            return;
        }
        logger.info("No 1.4-6: verify configurations");
        if (storedSettingMap.size() != clusterSettingMap.size()) {
            logger.error("    verify failure, restored clusterConfig size not equal!");
            return;
        }
        for (Map.Entry<String, ClusterSettingEntity> qryEntry : storedSettingMap.entrySet()) {
            ClusterSettingEntity targetEntity = clusterSettingMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored clusterConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                return;
            }
        }
        // b. read cluster configurations
        logger.info("No 2.1-6  recovery broker configurations");
        Map<String, BrokerConfEntity> storedBrokerConfigMap =
                (Map<String, BrokerConfEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "brokerConfig");
        if (storedBrokerConfigMap == null) {
            logger.error(strBuff
                    .append("    local broker configurations read failure!, brokerConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 2.2-6  upload broker configurations to master");
        if (!writeBrokerConfInfo(storedBrokerConfigMap, strBuff, result)) {
            logger.error(strBuff.append("    write broker configurations failure!")
                    .append(result.getErrMsg()).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 2.3-6  read restored broker configurations from master");
        Map<String, BrokerConfEntity> brokerConfMap = getBrokerConfInfos(strBuff);
        if (brokerConfMap == null) {
            logger.error("    read restored broker configurations failure!");
            return;
        }
        logger.info("No 2.4-6: verify configurations");
        if (brokerConfMap.size() != storedBrokerConfigMap.size()) {
            logger.error("    verify failure, restored brokerConfig size not equal!");
            return;
        }
        for (Map.Entry<String, BrokerConfEntity> qryEntry : storedBrokerConfigMap.entrySet()) {
            BrokerConfEntity targetEntity = brokerConfMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored brokerConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                return;
            }
        }
        // c. read topic control configurations
        logger.info("No 3.1-6  recovery topic control configurations");
        Map<String, TopicCtrlEntity> storedTopicCtrlMap =
                (Map<String, TopicCtrlEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "topicControlConfig");
        if (storedTopicCtrlMap == null) {
            logger.error(strBuff
                    .append("    local topic control configurations read failure!, topicControlConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 3.2-6  upload topic control configurations to master");
        if (!writeTopicCtrlInfo(storedTopicCtrlMap, strBuff, result)) {
            logger.error(strBuff.append("    write topic control configurations failure!")
                    .append(result.getErrMsg()).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 3.3-6  read restored topic control configurations from master");
        Map<String, TopicCtrlEntity> topicCtrlMap = getTopicControlInfos(strBuff);
        if (topicCtrlMap == null) {
            logger.error("    download topic control configure is null!");
            return;
        }
        logger.info("No 3.4-6: verify configurations");
        if (topicCtrlMap.size() != storedTopicCtrlMap.size()) {
            logger.error("    verify failure, restored topicControlConfig size not equal!");
            return;
        }
        for (Map.Entry<String, TopicCtrlEntity> qryEntry : storedTopicCtrlMap.entrySet()) {
            TopicCtrlEntity targetEntity = topicCtrlMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored topicControlConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                return;
            }
        }
        // d. read topic deploy configurations
        logger.info("No 4.1-6  recovery topic deploy configurations");
        Map<String, TopicDeployEntity> storedTopicDeployMap =
                (Map<String, TopicDeployEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "topicDeployConfig");
        if (storedTopicDeployMap == null) {
            logger.error(strBuff
                    .append("    local topic deploy configurations read failure!, topicDeployConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 4.2-6  upload topic deploy configurations to master");
        if (!writeTopicDeployInfo(storedTopicDeployMap, strBuff, result)) {
            logger.error(strBuff.append("    write topic deploy configurations failure!")
                    .append(result.getErrMsg()).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 4.3-6  read restored topic deploy configurations from master");
        Map<String, TopicDeployEntity> topicDeployMap = getTopicDeployInfos(strBuff);
        if (topicDeployMap == null) {
            logger.error("    download topic deploy configurations is null!");
            return;
        }
        logger.info("No 4.4-6: verify configurations");
        if (topicDeployMap.size() != storedTopicDeployMap.size()) {
            logger.error("    verify failure, stored topicDeployConfig size not equal!");
            return;
        }
        for (Map.Entry<String, TopicDeployEntity> qryEntry : storedTopicDeployMap.entrySet()) {
            TopicDeployEntity targetEntity = topicDeployMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored topicDeployConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                return;
            }
        }
        // e. group control configurations
        logger.info("No 5.1-6  recovery group control configurations");
        Map<String, GroupResCtrlEntity> storedGroupCtrlMap =
                (Map<String, GroupResCtrlEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "groupCtrlConfig");
        if (storedGroupCtrlMap == null) {
            logger.error(strBuff
                    .append("    local group control configurations read failure!, groupCtrlConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 5.2-6  upload group control configurations to master");
        if (!writeGroupResCtrlInfo(storedGroupCtrlMap, strBuff, result)) {
            logger.error(strBuff.append("    write group control configurations failure!")
                    .append(result.getErrMsg()).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 5.3-6  read restored group control configurations from master");
        Map<String, GroupResCtrlEntity> groupCtrlMap = getGroupResCtrlInfos(strBuff);
        if (groupCtrlMap == null) {
            logger.error("    download group control configurations is null!");
            return;
        }
        logger.info("No 5.4-6: verify configurations");
        if (groupCtrlMap.size() != storedGroupCtrlMap.size()) {
            logger.error("    verify failure, stored groupCtrlConfig size not equal!");
            return;
        }
        for (Map.Entry<String, GroupResCtrlEntity> qryEntry : storedGroupCtrlMap.entrySet()) {
            GroupResCtrlEntity targetEntity = groupCtrlMap.get(qryEntry.getKey());
            if (targetEntity == null || !targetEntity.isDataEquals(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored groupCtrlConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());
                return;
            }
        }
        // f. group consume control configurations
        logger.info("No 6.1-6  recovery group consume control configurations");
        Map<String, GroupConsumeCtrlEntity> storedGroupCsmMap =
                (Map<String, GroupConsumeCtrlEntity>) readObjectFromFile(
                        backupAndRecoveryPath, "groupConsumeConfig");
        if (storedGroupCsmMap == null) {
            logger.error(strBuff
                    .append("    local group consume configurations read failure!, groupConsumeConfig in ")
                    .append(backupAndRecoveryPath).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 6.2-6  upload group consume control configurations to master");
        if (!writeGroupCsmInfo(storedGroupCsmMap, strBuff, result)) {
            logger.error(strBuff.append("    write group consume control configurations failure!")
                    .append(result.getErrMsg()).toString());
            strBuff.delete(0, strBuff.length());
            return;
        }
        logger.info("No 6.3-6  read restored group consume control configurations from master");
        Map<String, GroupConsumeCtrlEntity> groupCsmInfoMap = getGroupCsmCtrlInfos(strBuff);
        if (groupCsmInfoMap == null) {
            logger.error("    download group consume configurations is null!");
            return;
        }
        logger.info("No 6.4-6: verify configurations");
        if (groupCsmInfoMap.size() != storedGroupCsmMap.size()) {
            logger.error("    verify failure, stored groupConsumeConfig size not equal!");
            return;
        }
        for (Map.Entry<String, GroupConsumeCtrlEntity> qryEntry : storedGroupCsmMap.entrySet()) {
            GroupConsumeCtrlEntity targetEntity = groupCsmInfoMap.get(qryEntry.getKey());
            if (targetEntity == null
                    || !targetEntity.isDataEquals(qryEntry.getValue())
                    || !targetEntity.isMatched(qryEntry.getValue())) {
                logger.error(strBuff
                        .append("    verify failure, stored groupCtrlConfig value not equal!")
                        .append(" data in server is ").append(qryEntry.getValue().toString())
                        .append(", data stored is ").append((targetEntity == null)
                                ? null : targetEntity.toString()).toString());

                logger.error("    verify failure, stored groupConsumeConfig value not equal!");
                return;
            }
        }
        logger.info("Recovery finished!");

    }

    /**
     * Query cluster setting configurations.
     *
     * @param strBuff  the string buffer
     * @return         the query result, null if query failure
     */
    private Map<String, ClusterSettingEntity> getClusterConfInfo(StringBuilder strBuff) {
        // http://127.0.0.1:8080/webapi.htm?method=admin_query_cluster_default_setting
        JsonObject jsonRes = qryDataFromMaster(
                "admin_query_cluster_default_setting", new HashMap<>(), strBuff);
        // check return result
        if (!jsonRes.get("result").getAsBoolean()) {
            logger.info(strBuff.append("Query cluster configurations info failure:")
                    .append(jsonRes.get("result").getAsString()).toString());
            strBuff.delete(0, strBuff.length());
            return null;
        }
        Map<String, ClusterSettingEntity> clusterSettingMap = new HashMap<>();
        JsonArray clusterInfoList = jsonRes.get("data").getAsJsonArray();
        for (int i = 0; i < clusterInfoList.size(); i++) {
            JsonObject jsonItem = clusterInfoList.get(i).getAsJsonObject();
            if (jsonItem == null) {
                continue;
            }
            try {
                // get base information
                BaseEntity baseEntity = getBaseEntityInfo(jsonItem);
                // get topic default configurations
                TopicPropGroup defTopicProps = getTopicProps(jsonItem);
                // get broker default configurations
                int brokerPort = jsonItem.get("brokerPort").getAsInt();
                int brokerTlsPort = jsonItem.get("brokerTLSPort").getAsInt();
                int brokerWebPort = jsonItem.get("brokerWebPort").getAsInt();
                int maxMsgSizeInMB = jsonItem.get("maxMsgSizeInMB").getAsInt();
                int qryPriorityId = jsonItem.get("qryPriorityId").getAsInt();
                boolean flowCtrlEnable = jsonItem.get("flowCtrlEnable").getAsBoolean();
                int flowRuleCnt = jsonItem.get("flowCtrlRuleCount").getAsInt();
                JsonArray flowCtrlInfoArray = jsonItem.get("flowCtrlInfo").getAsJsonArray();
                String flowCtrlInfoStr = flowCtrlInfoArray.toString();
                // build cluster setting entity
                ClusterSettingEntity settingEntity =
                        new ClusterSettingEntity(baseEntity);
                settingEntity.updModifyInfo(baseEntity.getDataVerId(),
                        brokerPort, brokerTlsPort, brokerWebPort, maxMsgSizeInMB,
                        qryPriorityId, flowCtrlEnable, flowRuleCnt, flowCtrlInfoStr, defTopicProps);
                clusterSettingMap.put(settingEntity.getRecordKey(), settingEntity);
            } catch (Throwable e) {
                logger.error(strBuff.append("Parse cluster configurations(")
                        .append(jsonItem.toString()).append(") throw exception ")
                        .append(e.toString()).toString());
                strBuff.delete(0, strBuff.length());
                throw e;
            }
        }
        return clusterSettingMap;
    }

    /**
     * Write cluster setting configurations to Master
     *
     * @param clusterConfMap  the cluster configures that needs to be stored
     * @param strBuff          the string buffer
     * @param result           the process result
     * @return         the process result
     */
    private boolean writeClusterConfInfo(Map<String, ClusterSettingEntity> clusterConfMap,
                                         StringBuilder strBuff, ProcessResult result) {
        if (clusterConfMap.isEmpty()) {
            return true;
        }
        int count = 0;
        Map<String, String> inParamMap = new HashMap<>();
        for (ClusterSettingEntity entity : clusterConfMap.values()) {
            // build cluster setting configurations
            entity.getConfigureInfo(inParamMap, true);
            if (!writeDataToMaster("admin_set_cluster_default_setting",
                    authToken, inParamMap, strBuff, result)) {
                return false;
            }
            inParamMap.clear();
        }
        return true;
    }

    /**
     * Query broker configurations
     *
     * @param strBuff  the string buffer
     * @return         the query result, null if query failure
     */
    private Map<String, BrokerConfEntity> getBrokerConfInfos(StringBuilder strBuff) {
        // http://127.0.0.1:8080/webapi.htm?method=admin_query_broker_configure
        JsonObject jsonRes = qryDataFromMaster(
                "admin_query_broker_configure", new HashMap<>(), strBuff);
        // check return result
        if (!jsonRes.get("result").getAsBoolean()) {
            logger.info(strBuff.append("Query broker configurations info failure:")
                    .append(jsonRes.get("result").getAsString()).toString());
            strBuff.delete(0, strBuff.length());
            return null;
        }
        Map<String, BrokerConfEntity> brokerInfoMap = new HashMap<>();
        JsonArray jsonBrokerInfoList = jsonRes.get("data").getAsJsonArray();
        for (int i = 0; i < jsonBrokerInfoList.size(); i++) {
            JsonObject jsonItem = jsonBrokerInfoList.get(i).getAsJsonObject();
            if (jsonItem == null) {
                continue;
            }
            try {
                // get base information
                BaseEntity baseEntity = getBaseEntityInfo(jsonItem);
                // get broker configurations
                int brokerId = jsonItem.get("brokerId").getAsInt();
                String brokerIp = jsonItem.get("brokerIp").getAsString();
                BrokerConfEntity brokerConfEntity =
                        new BrokerConfEntity(baseEntity, brokerId, brokerIp);
                // get topic configurations
                TopicPropGroup topicPropGroup = getTopicProps(jsonItem);
                // get broker configurations
                int brokerPort = jsonItem.get("brokerPort").getAsInt();
                int brokerTlsPort = jsonItem.get("brokerTLSPort").getAsInt();
                int brokerWebPort = jsonItem.get("brokerWebPort").getAsInt();
                int regionId = jsonItem.get("regionId").getAsInt();
                int groupId = jsonItem.get("groupId").getAsInt();
                String statusInfo = jsonItem.get("manageStatus").getAsString();
                ManageStatus mngStatus = ManageStatus.descOf(statusInfo);
                // build broker configurations
                brokerConfEntity.updModifyInfo(baseEntity.getDataVerId(),
                        brokerPort, brokerTlsPort, brokerWebPort, regionId,
                        groupId, mngStatus, topicPropGroup);
                brokerInfoMap.put(brokerIp, brokerConfEntity);
            } catch (Throwable e) {
                logger.error(strBuff.append("Parse broker configurations(")
                        .append(jsonItem.toString()).append(") throw exception ")
                        .append(e.toString()).toString());
                strBuff.delete(0, strBuff.length());
                throw e;
            }
        }
        return brokerInfoMap;
    }

    /**
     * Write broker configurations to Master
     *
     * @param brokerConfigMap  the broker configurations that needs to be stored
     * @param strBuff          the string buffer
     * @param result           the process result
     * @return         the process result
     */
    private boolean writeBrokerConfInfo(Map<String, BrokerConfEntity> brokerConfigMap,
                                        StringBuilder strBuff, ProcessResult result) {
        if (brokerConfigMap.isEmpty()) {
            return true;
        }
        int count = 0;
        Map<String, String> inParamMap = new HashMap<>();
        for (BrokerConfEntity entity : brokerConfigMap.values()) {
            if (count++ > 0) {
                strBuff.append(",");
            }
            // build broker configurations in json format
            entity.toWebJsonStr(strBuff, false, false, true, true);
            if (strBuff.length() > maxDataLength
                    || count % TServerConstants.CFG_BATCH_BROKER_OPERATE_MAX_COUNT == 0) {
                inParamMap.put("brokerJsonSet", "[" + strBuff.toString() + "]");
                strBuff.delete(0, strBuff.length());
                inParamMap.put("createUser", curOperator);
                inParamMap.put("modifyUser", curOperator);
                if (!writeDataToMaster("admin_batch_add_broker_configure",
                        authToken, inParamMap, strBuff, result)) {
                    return false;
                }
                count = 0;
                inParamMap.clear();
                strBuff.delete(0, strBuff.length());
            }
        }
        if (strBuff.length() > 0) {
            inParamMap.put("brokerJsonSet", "[" + strBuff.toString() + "]");
            strBuff.delete(0, strBuff.length());
            inParamMap.put("createUser", curOperator);
            inParamMap.put("modifyUser", curOperator);
            if (!writeDataToMaster("admin_batch_add_broker_configure",
                    authToken, inParamMap, strBuff, result)) {
                return false;
            }
            strBuff.delete(0, strBuff.length());
        }
        return true;
    }

    /**
     * Query topic control configurations
     *
     * @param strBuff  the string buffer
     * @return         the query result, null if query failure
     */
    private Map<String, TopicCtrlEntity> getTopicControlInfos(StringBuilder strBuff) {
        // http://127.0.0.1:8080/webapi.htm?method=admin_query_topic_control_info
        JsonObject jsonRes = qryDataFromMaster(
                "admin_query_topic_control_info", new HashMap<>(), strBuff);
        // check return result
        if (!jsonRes.get("result").getAsBoolean()) {
            logger.info(strBuff.append("Query topic control configurations info failure:")
                    .append(jsonRes.get("result").getAsString()).toString());
            strBuff.delete(0, strBuff.length());
            return null;
        }
        Map<String, TopicCtrlEntity> topicCtrlMap = new HashMap<>();
        JsonArray jsonTopicCtrlList = jsonRes.get("data").getAsJsonArray();
        for (int i = 0; i < jsonTopicCtrlList.size(); i++) {
            JsonObject jsonItem = jsonTopicCtrlList.get(i).getAsJsonObject();
            if (jsonItem == null) {
                continue;
            }
            try {
                // get base information
                BaseEntity baseEntity = getBaseEntityInfo(jsonItem);
                // get topic control configurations
                String topicName = jsonItem.get("topicName").getAsString();
                int topicNameId = jsonItem.get("topicNameId").getAsInt();
                boolean enableAuthCtrl = jsonItem.get("enableAuthControl").getAsBoolean();
                int maxMsgSizeInMB = jsonItem.get("maxMsgSizeInMB").getAsInt();
                // build topic control entity
                TopicCtrlEntity topicCtrlEntity = new TopicCtrlEntity(baseEntity, topicName);
                topicCtrlEntity.updModifyInfo(baseEntity.getDataVerId(),
                        topicNameId, maxMsgSizeInMB, enableAuthCtrl);
                topicCtrlMap.put(topicCtrlEntity.getTopicName(), topicCtrlEntity);
            } catch (Throwable e) {
                logger.error(strBuff.append("Parse topic control configurations(")
                        .append(jsonItem.toString()).append(") throw exception ")
                        .append(e.toString()).toString());
                strBuff.delete(0, strBuff.length());
                throw e;
            }
        }
        return topicCtrlMap;
    }

    /**
     * Write topic control configurations to Master
     *
     * @param topicCtrlMap     the topic control configurations that needs to be stored
     * @param strBuff          the string buffer
     * @param result           the process result
     * @return         the process result
     */
    private boolean writeTopicCtrlInfo(Map<String, TopicCtrlEntity> topicCtrlMap,
                                       StringBuilder strBuff, ProcessResult result) {
        if (topicCtrlMap.isEmpty()) {
            return true;
        }
        int count = 0;
        Map<String, String> inParamMap = new HashMap<>();
        for (TopicCtrlEntity entity : topicCtrlMap.values()) {
            if (count++ > 0) {
                strBuff.append(",");
            }
            // build topic control configurations in json format
            entity.toWebJsonStr(strBuff, true, true);
            if (strBuff.length() > maxDataLength
                    || count % TServerConstants.CFG_BATCH_BROKER_OPERATE_MAX_COUNT == 0) {
                inParamMap.put("topicCtrlJsonSet", "[" + strBuff.toString() + "]");
                strBuff.delete(0, strBuff.length());
                inParamMap.put("createUser", curOperator);
                inParamMap.put("modifyUser", curOperator);
                if (!writeDataToMaster("admin_batch_add_topic_control_info",
                        authToken, inParamMap, strBuff, result)) {
                    return false;
                }
                count = 0;
                inParamMap.clear();
                strBuff.delete(0, strBuff.length());
            }
        }
        if (strBuff.length() > 0) {
            inParamMap.put("topicCtrlJsonSet", "[" + strBuff.toString() + "]");
            strBuff.delete(0, strBuff.length());
            inParamMap.put("createUser", curOperator);
            inParamMap.put("modifyUser", curOperator);
            if (!writeDataToMaster("admin_batch_add_topic_control_info",
                    authToken, inParamMap, strBuff, result)) {
                return false;
            }
            strBuff.delete(0, strBuff.length());
        }
        return true;
    }

    /**
     * Query topic deploy configurations
     *
     * @param strBuff  the string buffer
     * @return         the query result, null if query failure
     */
    private Map<String, TopicDeployEntity> getTopicDeployInfos(StringBuilder strBuff) {
        // http://127.0.0.1:8080/webapi.htm?method=admin_query_topic_deploy_configure
        JsonObject jsonRes = qryDataFromMaster(
                "admin_query_topic_deploy_configure", new HashMap<>(), strBuff);
        // check return result
        if (!jsonRes.get("result").getAsBoolean()) {
            logger.info(strBuff.append("Query topic deploy configurations info failure:")
                    .append(jsonRes.get("result").getAsString()).toString());
            strBuff.delete(0, strBuff.length());
            return null;
        }
        Map<String, TopicDeployEntity> topicDeployMap = new HashMap<>();
        JsonArray jsonTopicDeployList = jsonRes.get("data").getAsJsonArray();
        for (int i = 0; i < jsonTopicDeployList.size(); i++) {
            JsonObject jsonItem = jsonTopicDeployList.get(i).getAsJsonObject();
            if (jsonItem == null) {
                continue;
            }
            try {
                // get base information
                BaseEntity baseEntity = getBaseEntityInfo(jsonItem);
                // get topic configurations
                TopicPropGroup topicPropGroup = getTopicProps(jsonItem);
                // get topic deploy configurations
                String topicName = jsonItem.get("topicName").getAsString();
                int brokerId = jsonItem.get("brokerId").getAsInt();
                int topicNameId = jsonItem.get("topicNameId").getAsInt();
                String brokerIp = jsonItem.get("brokerIp").getAsString();
                int brokerPort = jsonItem.get("brokerPort").getAsInt();
                int topicStatusId = jsonItem.get("topicStatusId").getAsInt();
                TopicStatus deployStatus = TopicStatus.valueOf(topicStatusId);
                // build topic deploy entity
                TopicDeployEntity topicDeployEntity =
                        new TopicDeployEntity(baseEntity, brokerId, topicName);
                topicDeployEntity.updModifyInfo(baseEntity.getDataVerId(),
                        topicNameId, brokerPort, brokerIp, deployStatus,
                        topicPropGroup);
                topicDeployMap.put(topicDeployEntity.getRecordKey(), topicDeployEntity);
            } catch (Throwable e) {
                logger.error(strBuff.append("Parse topic deploy configurations(")
                        .append(jsonItem.toString()).append(") throw exception ")
                        .append(e.toString()).toString());
                strBuff.delete(0, strBuff.length());
                throw e;
            }
        }
        return topicDeployMap;
    }

    /**
     * Write topic deploy configurations to Master
     *
     * @param topicDeployMap     the topic deploy configurations that needs to be stored
     * @param strBuff          the string buffer
     * @param result           the process result
     * @return         the process result
     */
    private boolean writeTopicDeployInfo(Map<String, TopicDeployEntity> topicDeployMap,
                                         StringBuilder strBuff, ProcessResult result) {
        if (topicDeployMap.isEmpty()) {
            return true;
        }
        int count = 0;
        Map<String, String> inParamMap = new HashMap<>();
        for (TopicDeployEntity entity : topicDeployMap.values()) {
            if (count++ > 0) {
                strBuff.append(",");
            }
            // build topic deploy configurations in json format
            entity.toWebJsonStr(strBuff, true, true);
            if (strBuff.length() > maxDataLength
                    || count % TServerConstants.CFG_BATCH_BROKER_OPERATE_MAX_COUNT == 0) {
                inParamMap.put("topicJsonSet", "[" + strBuff.toString() + "]");
                strBuff.delete(0, strBuff.length());
                inParamMap.put("createUser", curOperator);
                inParamMap.put("modifyUser", curOperator);
                if (!writeDataToMaster("admin_bath_add_topic_deploy_info",
                        authToken, inParamMap, strBuff, result)) {
                    return false;
                }
                count = 0;
                inParamMap.clear();
                strBuff.delete(0, strBuff.length());
            }
        }
        if (strBuff.length() > 0) {
            inParamMap.put("topicJsonSet", "[" + strBuff.toString() + "]");
            strBuff.delete(0, strBuff.length());
            inParamMap.put("createUser", curOperator);
            inParamMap.put("modifyUser", curOperator);
            if (!writeDataToMaster("admin_bath_add_topic_deploy_info",
                    authToken, inParamMap, strBuff, result)) {
                return false;
            }
            strBuff.delete(0, strBuff.length());
        }
        return true;
    }

    /**
     * Query group resource control configurations
     *
     * @param strBuff  the string buffer
     * @return         the query result, null if query failure
     */
    private Map<String, GroupResCtrlEntity> getGroupResCtrlInfos(StringBuilder strBuff) {
        // http://127.0.0.1:8080/webapi.htm?method=admin_query_group_resctrl_info
        JsonObject jsonRes = qryDataFromMaster(
                "admin_query_group_resctrl_info", new HashMap<>(), strBuff);
        // check return result
        if (!jsonRes.get("result").getAsBoolean()) {
            logger.info(strBuff.append("Query group resource control configurations info failure:")
                    .append(jsonRes.get("result").getAsString()).toString());
            strBuff.delete(0, strBuff.length());
            return null;
        }
        Map<String, GroupResCtrlEntity> groupResCtrlMap = new HashMap<>();
        JsonArray jsonGroupResCtrlList = jsonRes.get("data").getAsJsonArray();
        for (int i = 0; i < jsonGroupResCtrlList.size(); i++) {
            JsonObject jsonItem = jsonGroupResCtrlList.get(i).getAsJsonObject();
            if (jsonItem == null) {
                continue;
            }
            try {
                // get base information
                BaseEntity baseEntity = getBaseEntityInfo(jsonItem);
                // get group resource control configurations
                String groupName = jsonItem.get("groupName").getAsString();
                boolean resCheckEnable = jsonItem.get("resCheckEnable").getAsBoolean();
                int alwdBCRate = jsonItem.get("alwdBrokerClientRate").getAsInt();
                int qryPriorityId = jsonItem.get("qryPriorityId").getAsInt();
                boolean flowCtrlEnable = jsonItem.get("flowCtrlEnable").getAsBoolean();
                int flowCtrlRuleCount = jsonItem.get("flowCtrlRuleCount").getAsInt();
                JsonArray flowCtrlInfoArray = jsonItem.get("flowCtrlInfo").getAsJsonArray();
                String flowCtrlInfoStr = flowCtrlInfoArray.toString();
                // build group resource control entity
                GroupResCtrlEntity groupResCtrlEntity =
                        new GroupResCtrlEntity(baseEntity, groupName);
                groupResCtrlEntity.updModifyInfo(baseEntity.getDataVerId(),
                        resCheckEnable, alwdBCRate, qryPriorityId,
                        flowCtrlEnable, flowCtrlRuleCount, flowCtrlInfoStr);
                groupResCtrlMap.put(groupResCtrlEntity.getGroupName(), groupResCtrlEntity);
            } catch (Throwable e) {
                logger.error(strBuff.append("Parse group resource control configurations(")
                        .append(jsonItem.toString()).append(") throw exception ")
                        .append(e.toString()).toString());
                strBuff.delete(0, strBuff.length());
                throw e;
            }
        }
        return groupResCtrlMap;
    }

    /**
     * Write group resource control configurations to Master
     *
     * @param groupResCtrlMap  the group resource control configurations that needs to be stored
     * @param strBuff          the string buffer
     * @param result           the process result
     * @return         the process result
     */
    private boolean writeGroupResCtrlInfo(Map<String, GroupResCtrlEntity> groupResCtrlMap,
                                          StringBuilder strBuff, ProcessResult result) {
        if (groupResCtrlMap.isEmpty()) {
            return true;
        }
        int count = 0;
        Map<String, String> inParamMap = new HashMap<>();
        for (GroupResCtrlEntity entity : groupResCtrlMap.values()) {
            if (count++ > 0) {
                strBuff.append(",");
            }
            // build group resource control configurations in json format
            entity.toWebJsonStr(strBuff, true, true);
            if (strBuff.length() > maxDataLength
                    || count % TServerConstants.CFG_BATCH_BROKER_OPERATE_MAX_COUNT == 0) {
                inParamMap.put("groupResCtrlJsonSet", "[" + strBuff.toString() + "]");
                strBuff.delete(0, strBuff.length());
                inParamMap.put("createUser", curOperator);
                inParamMap.put("modifyUser", curOperator);
                if (!writeDataToMaster("admin_batch_add_group_resctrl_info",
                        authToken, inParamMap, strBuff, result)) {
                    return false;
                }
                count = 0;
                inParamMap.clear();
                strBuff.delete(0, strBuff.length());
            }
        }
        if (strBuff.length() > 0) {
            inParamMap.put("groupResCtrlJsonSet", "[" + strBuff.toString() + "]");
            strBuff.delete(0, strBuff.length());
            inParamMap.put("createUser", curOperator);
            inParamMap.put("modifyUser", curOperator);
            if (!writeDataToMaster("admin_batch_add_group_resctrl_info",
                    authToken, inParamMap, strBuff, result)) {
                return false;
            }
            strBuff.delete(0, strBuff.length());
        }
        return true;
    }

    /**
     * Query group consume control configurations
     *
     * @param strBuff  the string buffer
     * @return         the query result, null if query failure
     */
    private Map<String, GroupConsumeCtrlEntity> getGroupCsmCtrlInfos(StringBuilder strBuff) {
        // http://127.0.0.1:8080/webapi.htm?method=admin_query_group_csmctrl_info
        JsonObject jsonRes = qryDataFromMaster(
                "admin_query_group_csmctrl_info", new HashMap<>(), strBuff);
        // check return result
        if (!jsonRes.get("result").getAsBoolean()) {
            logger.info(strBuff.append("Query group consume control configurations info failure:")
                    .append(jsonRes.get("result").getAsString()).toString());
            strBuff.delete(0, strBuff.length());
            return null;
        }
        Map<String, GroupConsumeCtrlEntity> groupCsmCtrlMap = new HashMap<>();
        JsonArray jsonGroupCsmCtrlList = jsonRes.get("data").getAsJsonArray();
        for (int i = 0; i < jsonGroupCsmCtrlList.size(); i++) {
            JsonObject jsonItem = jsonGroupCsmCtrlList.get(i).getAsJsonObject();
            if (jsonItem == null) {
                continue;
            }
            try {
                // get base information
                BaseEntity baseEntity = getBaseEntityInfo(jsonItem);
                // get group consume control configurations
                String groupName = jsonItem.get("groupName").getAsString();
                String topicName = jsonItem.get("topicName").getAsString();
                boolean consumeEnable = jsonItem.get("consumeEnable").getAsBoolean();
                String disableCsmRsn = jsonItem.get("disableCsmRsn").getAsString();
                boolean filterEnable = jsonItem.get("filterEnable").getAsBoolean();
                String filterConds = jsonItem.get("filterConds").getAsString();
                // build group consume control entity
                GroupConsumeCtrlEntity groupCsmCtrlEntity =
                        new GroupConsumeCtrlEntity(baseEntity, groupName, topicName);
                groupCsmCtrlEntity.updModifyInfo(baseEntity.getDataVerId(),
                        consumeEnable, disableCsmRsn, filterEnable, filterConds);
                groupCsmCtrlMap.put(groupCsmCtrlEntity.getRecordKey(), groupCsmCtrlEntity);
            } catch (Throwable e) {
                logger.error(strBuff.append("Parse group consume control configurations(")
                        .append(jsonItem.toString()).append(") throw exception ")
                        .append(e.toString()).toString());
                strBuff.delete(0, strBuff.length());
                throw e;
            }
        }
        return groupCsmCtrlMap;
    }

    /**
     * Write group consume control configurations to Master
     *
     * @param groupCsmMap  the group consume control configurations that needs to be stored
     * @param strBuff      the string buffer
     * @param result       the process result
     * @return         the process result
     */
    private boolean writeGroupCsmInfo(Map<String, GroupConsumeCtrlEntity> groupCsmMap,
                                      StringBuilder strBuff, ProcessResult result) {
        if (groupCsmMap.isEmpty()) {
            return true;
        }
        int count = 0;
        Map<String, String> inParamMap = new HashMap<>();
        for (GroupConsumeCtrlEntity entity : groupCsmMap.values()) {
            if (count++ > 0) {
                strBuff.append(",");
            }
            // build group consume control configurations in json format
            entity.toWebJsonStr(strBuff, true, true);
            if (strBuff.length() > maxDataLength
                    || count % TServerConstants.CFG_BATCH_BROKER_OPERATE_MAX_COUNT == 0) {
                inParamMap.put("groupCsmJsonSet", "[" + strBuff.toString() + "]");
                strBuff.delete(0, strBuff.length());
                inParamMap.put("createUser", curOperator);
                inParamMap.put("modifyUser", curOperator);
                if (!writeDataToMaster("admin_batch_add_group_csmctrl_info",
                        authToken, inParamMap, strBuff, result)) {
                    return false;
                }
                count = 0;
                inParamMap.clear();
                strBuff.delete(0, strBuff.length());
            }
        }
        if (strBuff.length() > 0) {
            inParamMap.put("groupCsmJsonSet", "[" + strBuff.toString() + "]");
            strBuff.delete(0, strBuff.length());
            inParamMap.put("createUser", curOperator);
            inParamMap.put("modifyUser", curOperator);
            if (!writeDataToMaster("admin_batch_add_group_csmctrl_info",
                    authToken, inParamMap, strBuff, result)) {
                return false;
            }
            strBuff.delete(0, strBuff.length());
        }
        return true;
    }

    /**
     * Get BaseEntity information from JsonObject
     *
     * @param jsonItem  the json object
     * @return         the BaseEntity object
     */
    private BaseEntity getBaseEntityInfo(JsonObject jsonItem) {
        long dataVersionId = jsonItem.get("dataVersionId").getAsInt();
        String createUser = jsonItem.get("createUser").getAsString();
        String createDateStr = jsonItem.get("createDate").getAsString();
        Date createDate = DateTimeConvertUtils.yyyyMMddHHmmss2date(createDateStr);
        String modifyUser = jsonItem.get("modifyUser").getAsString();
        String modifyDateStr = jsonItem.get("modifyDate").getAsString();
        Date modifyDate = DateTimeConvertUtils.yyyyMMddHHmmss2date(modifyDateStr);
        return new BaseEntity(dataVersionId, createUser, createDate, modifyUser, modifyDate);
    }

    /**
     * Get TopicPropGroup information from JsonObject
     *
     * @param jsonItem  the json object
     * @return         the TopicPropGroup object
     */
    private TopicPropGroup getTopicProps(JsonObject jsonItem) {
        int numTopicStores = jsonItem.get("numTopicStores").getAsInt();
        int numPartitions = jsonItem.get("numPartitions").getAsInt();
        int unflushThreshold = jsonItem.get("unflushThreshold").getAsInt();
        int unflushInterval = jsonItem.get("unflushInterval").getAsInt();
        int unflushDataHold = jsonItem.get("unflushDataHold").getAsInt();
        int memCacheMsgSizeInMB = jsonItem.get("memCacheMsgSizeInMB").getAsInt();
        int memCacheMsgCntInK = jsonItem.get("memCacheMsgCntInK").getAsInt();
        int memCacheFlushIntvl = jsonItem.get("memCacheFlushIntvl").getAsInt();
        boolean acceptPublish = jsonItem.get("acceptPublish").getAsBoolean();
        boolean acceptSubscribe = jsonItem.get("acceptSubscribe").getAsBoolean();
        String deletePolicy = jsonItem.get("deletePolicy").getAsString();
        int dataStoreType = jsonItem.get("dataStoreType").getAsInt();
        String dataPath = jsonItem.get("dataPath").getAsString();
        return new TopicPropGroup(numTopicStores, numPartitions,
                unflushThreshold, unflushInterval, unflushDataHold,
                memCacheMsgSizeInMB, memCacheMsgCntInK, memCacheFlushIntvl,
                acceptPublish, acceptSubscribe, deletePolicy, dataStoreType, dataPath);
    }

    /**
     * Query data from Masters
     *
     * @param method      the method name
     * @param inParamMap  the parameter map
     * @param strBuff     the string buffer
     * @return            the query result of Json format, null if failure
     */
    private JsonObject qryDataFromMaster(String method,
                                         Map<String, String> inParamMap,
                                         StringBuilder strBuff) {
        String visitUrl;
        JsonObject jsonRes = null;
        // call master nodes
        for (String address : masterInfo.getNodeHostPortList()) {
            visitUrl = strBuff.append("http://").append(address)
                    .append("/webapi.htm?method=").append(method).toString();
            strBuff.delete(0, strBuff.length());
            try {
                jsonRes = HttpUtils.requestWebService(visitUrl, inParamMap);
                if (jsonRes != null) {
                    // if get data, break cycle
                    break;
                }
            } catch (Throwable e) {
                //
            }
        }
        return jsonRes;
    }

    /**
     * Write data to Masters
     *
     * @param method      the method name
     * @param opCode      the operation code
     * @param inParamMap  the parameter map
     * @param strBuff     the string buffer
     * @param result      the process result
     * @return            success or failure
     */
    private boolean writeDataToMaster(String method, String opCode,
                                      Map<String, String> inParamMap,
                                      StringBuilder strBuff,
                                      ProcessResult result) {
        String visitUrl;
        JsonObject jsonRes = null;
        // call master nodes
        for (String address : masterInfo.getNodeHostPortList()) {
            visitUrl = strBuff.append("http://").append(address)
                    .append("/webapi.htm?method=").append(method)
                    .append("&confModAuthToken=").append(opCode).toString();
            strBuff.delete(0, strBuff.length());
            try {
                jsonRes = HttpUtils.requestWebService(visitUrl, inParamMap);
                if (jsonRes != null) {
                    // if get data, break cycle
                    break;
                }
            } catch (Throwable e) {
                //
            }
        }
        if (jsonRes == null) {
            result.setFailResult(400, "Connect master failure!");
        } else {
            if (jsonRes.get("result").getAsBoolean()) {
                result.setSuccResult();
            } else {
                result.setFailResult(jsonRes.get("errCode").getAsInt(),
                        jsonRes.get("errMsg").getAsString());
            }
        }
        return result.isSuccess();
    }

    /**
     * Read object data from specified file path
     *
     * @param configPath    the stored file path
     * @param fileName      the stored file name
     * @return              the stored object
     */
    private Object readObjectFromFile(String configPath, String fileName) {
        FileInputStream fis = null;
        ObjectInputStream is = null;
        try {
            File file = null;
            if (configPath.lastIndexOf(File.separator) != configPath.length() - 1) {
                file = new File(configPath + File.separator + fileName + ".conf");
            } else {
                file = new File(configPath + fileName + ".conf");
            }
            if (file.exists()) {
                fis = new FileInputStream(file);
                is = new ObjectInputStream(fis);
                Object entry = is.readObject();
                fis.close();
                return entry;
            } else {
                return null;
            }
        } catch (Throwable e1) {
            logger.error("store " + fileName + " exception", e1);
            return null;
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (Throwable e2) {
                    //
                }
            }
        }
    }

    /**
     * Write object data to specified file path
     *
     * @param storeObj      the object to be stored
     * @param configPath    the stored file path
     * @param fileName      the stored file name
     */
    private void storeObjectToFile(Object storeObj, String configPath, String fileName) {
        FileOutputStream fos = null;
        ObjectOutputStream p = null;
        try {
            File file = null;
            if (configPath.lastIndexOf(File.separator) != configPath.length() - 1) {
                file = new File(configPath + File.separator + fileName + ".conf");
            } else {
                file = new File(configPath + fileName + ".conf");
            }
            logger.info("Store address is " + file.getAbsolutePath());
            if (!file.getParentFile().exists()) {
                file.getParentFile().mkdir();
            }
            if (!file.exists()) {
                file.createNewFile();
            }
            fos = new FileOutputStream(file);
            p = new ObjectOutputStream(fos);
            p.writeObject(storeObj);
            p.flush();
        } catch (Throwable e) {
            logger.warn("store " + fileName + " exception", e);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (Throwable e2) {
                    //
                }
            }
        }
    }
}
