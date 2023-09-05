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

package org.apache.inlong.tubemq.server.tools.cli;

import org.apache.inlong.tubemq.server.common.fielddef.WebFieldDef;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Parameters(commandDescription = "Topic commands")
public class TopicCommand extends AbstractCommand {

    @Parameter()
    private List<String> params;

    final private static String[] requestMethod = new String[]{"--method", ""};

    final private static Map<String, Object> requestParams = new HashMap<>();

    final private static CliWebapiAdmin cliWebapiAdmin = new CliWebapiAdmin(requestParams);

    public TopicCommand() {
        super("topic");

        jcommander.addCommand("list", new TopicList());
        jcommander.addCommand("update", new TopicUpdate());
        jcommander.addCommand("create", new TopicCreate());
        jcommander.addCommand("delete", new TopicDelete());
    }

    @Parameters(commandDescription = "Topic List")
    private static class TopicList extends AbstractCommandRunner {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-n", "--topicName"}, order = 0, description = "String. Topic name")
        private String topicName;

        @Parameter(names = {"-sid", "--topicStatusId"}, order = 1, description = "Int. Topic status ID")
        private int topicStatusId = 0;

        @Parameter(names = {"-bid", "--brokerId"}, order = 2, description = "String. Brokers' ID, separated by commas")
        private String brokerId;

        @Parameter(names = {"-dp", "--deletePolicy"}, order = 3, description = "String. File aging strategy")
        private String deletePolicy;

        @Parameter(names = {"-np", "--numPartitions"}, order = 4, description = "Int. Number of partitions")
        private int numPartitions = 3;

        @Parameter(names = {"-nts", "--numTopicStores"}, order = 5, description = "Int. Number of topic stores")
        private int numTopicStores = 1;

        @Parameter(names = {"-uft",
                "--unflushThreshold"}, order = 6, description = "Int. Maximum allowed disk unflushing message count")
        private int unflushThreshold = 1000;

        @Parameter(names = {"-ufi",
                "--unflushInterval"}, order = 7, description = "Int. Maximum allowed disk unflushing interval")
        private int unflushInterval = 10000;

        @Parameter(names = {"-ufd",
                "--unflushDataHold"}, order = 8, description = "Int. Maximum allowed disk unflushing data size")
        private int unflushDataHold = 0;

        @Parameter(names = {"-mc",
                "--memCacheMsgCntInK"}, order = 9, description = "Int. Maximum allowed memory cache unflushing message count")
        private int memCacheMsgCntInK = 10;

        @Parameter(names = {"-ms",
                "--memCacheMsgSizeInMB"}, order = 10, description = "Int. Maximum allowed memory cache size in MB")
        private int memCacheMsgSizeInMB = 2;

        @Parameter(names = {"-mfi",
                "--memCacheFlushIntvl"}, order = 11, description = "Int. Maximum allowed disk unflushing data size")
        private int memCacheFlushIntvl = 20000;

        @Parameter(names = {"-c", "--createUser"}, order = 12, description = "String. Record creator")
        private String createUser;

        @Parameter(names = {"-m", "--modifyUser"}, order = 13, description = "String. Record modifier")
        private String modifyUser;

        @Override
        void run() {
            try {
                requestMethod[1] = "admin_query_topic_info";
                requestParams.clear();
                if (topicName != null)
                    requestParams.put(WebFieldDef.TOPICNAME.name, topicName);
                requestParams.put(WebFieldDef.TOPICSTATUSID.name, topicStatusId);
                if (brokerId != null)
                    requestParams.put(WebFieldDef.BROKERID.name, brokerId);
                if (deletePolicy != null)
                    requestParams.put(WebFieldDef.DELETEPOLICY.name, deletePolicy);
                requestParams.put(WebFieldDef.NUMPARTITIONS.name, numPartitions);
                requestParams.put(WebFieldDef.NUMTOPICSTORES.name, numTopicStores);
                requestParams.put(WebFieldDef.UNFLUSHTHRESHOLD.name, unflushThreshold);
                requestParams.put(WebFieldDef.UNFLUSHINTERVAL.name, unflushInterval);
                requestParams.put(WebFieldDef.UNFLUSHDATAHOLD.name, unflushDataHold);
                requestParams.put(WebFieldDef.UNFMCACHECNTINK.name, memCacheMsgCntInK);
                requestParams.put(WebFieldDef.MCACHESIZEINMB.name, memCacheMsgSizeInMB);
                requestParams.put(WebFieldDef.UNFMCACHEINTERVAL.name, memCacheFlushIntvl);
                if (createUser != null)
                    requestParams.put(WebFieldDef.CREATEUSER.name, createUser);
                if (modifyUser != null)
                    requestParams.put(WebFieldDef.MODIFYUSER.name, modifyUser);
                cliWebapiAdmin.processParams(requestMethod);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Topic Update")
    private static class TopicUpdate extends AbstractCommandRunner {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-n", "--topicName"}, order = 0, required = true, description = "String. Topic name")
        private String topicName;

        @Parameter(names = {"-bid",
                "--brokerId"}, order = 1, required = true, description = "String. Brokers' ID, separated by commas")
        private String brokerId;

        @Parameter(names = {"-dp", "--deletePolicy"}, order = 4, description = "String. File aging strategy")
        private String deletePolicy;

        @Parameter(names = {"-np", "--numPartitions"}, order = 5, description = "Int. Number of partitions")
        private int numPartitions = 3;

        @Parameter(names = {"-uft",
                "--unflushThreshold"}, order = 6, description = "Int. Maximum allowed disk unflushing message count")
        private int unflushThreshold = 1000;

        @Parameter(names = {"-ufi",
                "--unflushInterval"}, order = 7, description = "Int. Maximum allowed disk unflushing interval")
        private int unflushInterval = 10000;

        @Parameter(names = {"-ufd",
                "--unflushDataHold"}, order = 8, description = "Int. Maximum allowed disk unflushing data size")
        private int unflushDataHold = 0;

        @Parameter(names = {"-nts", "--numTopicStores"}, order = 9, description = "Int. Number of topic stores")
        private int numTopicStores = 1;

        @Parameter(names = {"-mc",
                "--memCacheMsgCntInK"}, order = 10, description = "Int. Maximum allowed memory cache unflushing message count")
        private int memCacheMsgCntInK = 10;

        @Parameter(names = {"-ms",
                "--memCacheMsgSizeInMB"}, order = 11, description = "Int. Maximum allowed memory cache size in MB")
        private int memCacheMsgSizeInMB = 2;

        @Parameter(names = {"-mfi",
                "--memCacheFlushIntvl"}, order = 12, description = "Int. Maximum allowed disk unflushing data size")
        private int memCacheFlushIntvl = 20000;

        @Parameter(names = {"-ap", "--acceptPublish"}, order = 13, description = "Boolean. Enable publishing")
        private boolean acceptPublish = true;

        @Parameter(names = {"-as", "--acceptSubscribe"}, order = 14, description = "Boolean. Enable subscription")
        private boolean acceptSubscribe = true;

        @Parameter(names = {"-mms",
                "--maxMsgSizeInMB"}, order = 15, description = "Int. Maximum allowed message length, unit MB")
        private int maxMsgSizeInMB = 1;

        @Parameter(names = {"-m", "--modifyUser"}, order = 2, required = true, description = "String. Record modifier")
        private String modifyUser;

        @Parameter(names = {"-md", "--modifyDate"}, order = 16, description = "String. Record modification date")
        private String modifyDate;

        @Parameter(names = {"-at",
                "--confModAuthToken"}, order = 3, required = true, description = "String. Admin api operation authorization code")
        private String confModAuthToken;

        @Override
        void run() {
            try {
                requestMethod[1] = "admin_modify_topic_info";
                requestParams.clear();
                if (topicName != null)
                    requestParams.put(WebFieldDef.TOPICNAME.name, topicName);
                if (brokerId != null)
                    requestParams.put(WebFieldDef.BROKERID.name, brokerId);
                if (deletePolicy != null)
                    requestParams.put(WebFieldDef.DELETEPOLICY.name, deletePolicy);
                requestParams.put(WebFieldDef.NUMPARTITIONS.name, numPartitions);
                requestParams.put(WebFieldDef.UNFLUSHTHRESHOLD.name, unflushThreshold);
                requestParams.put(WebFieldDef.UNFLUSHINTERVAL.name, unflushInterval);
                requestParams.put(WebFieldDef.UNFLUSHDATAHOLD.name, unflushDataHold);
                requestParams.put(WebFieldDef.NUMTOPICSTORES.name, numTopicStores);
                requestParams.put(WebFieldDef.UNFMCACHECNTINK.name, memCacheMsgCntInK);
                requestParams.put(WebFieldDef.MCACHESIZEINMB.name, memCacheMsgSizeInMB);
                requestParams.put(WebFieldDef.UNFMCACHEINTERVAL.name, memCacheFlushIntvl);
                requestParams.put(WebFieldDef.ACCEPTPUBLISH.name, acceptPublish);
                requestParams.put(WebFieldDef.ACCEPTSUBSCRIBE.name, acceptSubscribe);
                requestParams.put(WebFieldDef.MAXMSGSIZEINMB.name, maxMsgSizeInMB);
                if (modifyUser != null)
                    requestParams.put(WebFieldDef.MODIFYUSER.name, modifyUser);
                if (modifyDate != null)
                    requestParams.put(WebFieldDef.MODIFYDATE.name, modifyDate);
                if (confModAuthToken != null)
                    requestParams.put(WebFieldDef.ADMINAUTHTOKEN.name, confModAuthToken);
                cliWebapiAdmin.processParams(requestMethod);

                System.out.println("Reloading broker configure...");
                requestParams.clear();
                requestMethod[1] = "admin_reload_broker_configure";
                if (brokerId != null)
                    requestParams.put(WebFieldDef.BROKERID.name, brokerId);
                if (modifyUser != null)
                    requestParams.put(WebFieldDef.MODIFYUSER.name, modifyUser);
                if (modifyDate != null)
                    requestParams.put(WebFieldDef.MODIFYDATE.name, modifyDate);
                if (confModAuthToken != null)
                    requestParams.put(WebFieldDef.ADMINAUTHTOKEN.name, confModAuthToken);
                cliWebapiAdmin.processParams(requestMethod);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Topic Create")
    private static class TopicCreate extends AbstractCommandRunner {

        @Parameter()
        private List<String> params = new ArrayList<>();

        @Parameter(names = {"-n", "--topicName"}, order = 0, required = true, description = "String. Topic name")
        private String topicName;

        @Parameter(names = {"-bid",
                "--brokerId"}, order = 1, required = true, description = "String. Brokers' ID, separated by commas")
        private String brokerId;

        @Parameter(names = {"-dp", "--deletePolicy"}, order = 4, description = "String. File aging strategy")
        private String deletePolicy;

        @Parameter(names = {"-np", "--numPartitions"}, order = 5, description = "Int. Number of partitions")
        private int numPartitions = -1;

        @Parameter(names = {"-uft",
                "--unflushThreshold"}, order = 6, description = "Int. Maximum allowed disk unflushing message count")
        private int unflushThreshold = -1;

        @Parameter(names = {"-ufi",
                "--unflushInterval"}, order = 7, description = "Int. Maximum allowed disk unflushing interval")
        private int unflushInterval = -1;

        @Parameter(names = {"-ufd",
                "--unflushDataHold"}, order = 8, description = "Int. Maximum allowed disk unflushing data size")
        private int unflushDataHold = 0;

        @Parameter(names = {"-nts", "--numTopicStores"}, order = 9, description = "Int. Number of topic stores")
        private int numTopicStores = 1;

        @Parameter(names = {"-mc",
                "--memCacheMsgCntInK"}, order = 10, description = "Int. Maximum allowed memory cache unflushing message count")
        private int memCacheMsgCntInK = 10;

        @Parameter(names = {"-ms",
                "--memCacheMsgSizeInMB"}, order = 11, description = "Int. Maximum allowed memory cache size in MB")
        private int memCacheMsgSizeInMB = 2;

        @Parameter(names = {"-mfi",
                "--memCacheFlushIntvl"}, order = 12, description = "Int. Maximum allowed disk unflushing data size")
        private int memCacheFlushIntvl = 20000;

        @Parameter(names = {"-ap", "--acceptPublish"}, order = 13, description = "Boolean. Enable publishing")
        private boolean acceptPublish = true;

        @Parameter(names = {"-as", "--acceptSubscribe"}, order = 14, description = "Boolean. Enable subscription")
        private boolean acceptSubscribe = true;

        @Parameter(names = {"-mms",
                "--maxMsgSizeInMB"}, order = 15, description = "Int. Maximum allowed message length, unit MB")
        private int maxMsgSizeInMB = 1;

        @Parameter(names = {"-c", "--createUser"}, order = 2, required = true, description = "String. Record creator")
        private String createUser;

        @Parameter(names = {"-cd", "--createDate"}, order = 16, description = "String. Record creation date")
        private String createDate;

        @Parameter(names = {"-at",
                "--confModAuthToken"}, order = 3, required = true, description = "String.Admin api operation authorization code")
        private String confModAuthToken;

        @Override
        void run() {
            try {
                requestMethod[1] = "admin_add_new_topic_record";
                requestParams.clear();
                if (topicName != null)
                    requestParams.put(WebFieldDef.TOPICNAME.name, topicName);
                if (brokerId != null)
                    requestParams.put(WebFieldDef.BROKERID.name, brokerId);
                if (deletePolicy != null)
                    requestParams.put(WebFieldDef.DELETEPOLICY.name, deletePolicy);
                if (numPartitions != -1)
                    requestParams.put(WebFieldDef.NUMPARTITIONS.name, numPartitions);
                if (unflushThreshold != -1)
                    requestParams.put(WebFieldDef.UNFLUSHTHRESHOLD.name, unflushThreshold);
                if (unflushInterval != -1)
                    requestParams.put(WebFieldDef.UNFLUSHINTERVAL.name, unflushInterval);
                requestParams.put(WebFieldDef.UNFLUSHDATAHOLD.name, unflushDataHold);
                requestParams.put(WebFieldDef.NUMTOPICSTORES.name, numTopicStores);
                requestParams.put(WebFieldDef.UNFMCACHECNTINK.name, memCacheMsgCntInK);
                requestParams.put(WebFieldDef.MCACHESIZEINMB.name, memCacheMsgSizeInMB);
                requestParams.put(WebFieldDef.UNFMCACHEINTERVAL.name, memCacheFlushIntvl);
                requestParams.put(WebFieldDef.ACCEPTPUBLISH.name, acceptPublish);
                requestParams.put(WebFieldDef.ACCEPTSUBSCRIBE.name, acceptSubscribe);
                requestParams.put(WebFieldDef.MAXMSGSIZEINMB.name, maxMsgSizeInMB);
                if (createUser != null)
                    requestParams.put(WebFieldDef.CREATEUSER.name, createUser);
                if (createDate != null)
                    requestParams.put(WebFieldDef.CREATEDATE.name, createDate);
                if (confModAuthToken != null)
                    requestParams.put(WebFieldDef.ADMINAUTHTOKEN.name, confModAuthToken);
                cliWebapiAdmin.processParams(requestMethod);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Topic Delete")
    private static class TopicDelete extends AbstractCommandRunner {

        @Parameter()
        private List<String> params = new ArrayList<>();

        @Parameter(names = {"-o",
                "--deleteOpt"}, order = 0, required = true, description = "Delete options, must in { soft | redo | hard }")
        private String deleteOpt = "soft";

        @Parameter(names = {"-n", "--topicName"}, order = 1, required = true, description = "String. Topic name")
        private String topicName;

        @Parameter(names = {"-bid",
                "--brokerId"}, order = 2, required = true, description = "String. Brokers' ID, separated by commas")
        private String brokerId;

        @Parameter(names = {"-m", "--modifyUser"}, order = 3, required = true, description = "String. Record modifier")
        private String modifyUser;

        @Parameter(names = {"-md", "--modifyDate"}, order = 5, description = "String. Record modification date")
        private String modifyDate;

        @Parameter(names = {"-at",
                "--confModAuthToken"}, order = 4, required = true, description = "String. Admin api operation authorization code")
        private String confModAuthToken;

        private void softDelete() throws Exception {
            System.out.println("Turning publish and subscribe status to false...");
            requestMethod[1] = "admin_modify_topic_info";
            requestParams.put(WebFieldDef.ACCEPTPUBLISH.name, false);
            requestParams.put(WebFieldDef.ACCEPTSUBSCRIBE.name, false);
            cliWebapiAdmin.processParams(requestMethod);
            requestParams.remove(WebFieldDef.ACCEPTPUBLISH.name);
            requestParams.remove(WebFieldDef.ACCEPTSUBSCRIBE.name);

            System.out.println("Beginning to delete...");
            requestMethod[1] = "admin_delete_topic_info";
            cliWebapiAdmin.processParams(requestMethod);
        }

        private void redoDelete() throws Exception {
            requestMethod[1] = "admin_redo_deleted_topic_info";
            cliWebapiAdmin.processParams(requestMethod);
<<<<<<< HEAD
=======

            // requestMethod[1] = "admin_modify_topic_info";
            // requestParams.put(WebFieldDef.ACCEPTPUBLISH.name, true);
            // requestParams.put(WebFieldDef.ACCEPTSUBSCRIBE.name, true);
            // cliTopicAdmin.processParams(requestMethod);
            // requestParams.remove(WebFieldDef.ACCEPTPUBLISH.name);
            // requestParams.remove(WebFieldDef.ACCEPTSUBSCRIBE.name);
>>>>>>> edbc5109eb47b42e0901328559ca9c016a55f050
        }

        private void hardDelete() throws Exception {
            softDelete();

            System.out.println("Beginning to hard delete...");
            requestMethod[1] = "admin_remove_topic_info";
            cliWebapiAdmin.processParams(requestMethod);
        }

        @Override
        void run() {
            try {
                requestParams.clear();
                if (topicName != null)
                    requestParams.put(WebFieldDef.TOPICNAME.name, topicName);
                if (brokerId != null)
                    requestParams.put(WebFieldDef.BROKERID.name, brokerId);
                if (modifyUser != null)
                    requestParams.put(WebFieldDef.MODIFYUSER.name, modifyUser);
                if (modifyDate != null)
                    requestParams.put(WebFieldDef.MODIFYDATE.name, modifyDate);
                if (confModAuthToken != null)
                    requestParams.put(WebFieldDef.ADMINAUTHTOKEN.name, confModAuthToken);
                switch (deleteOpt) {
                    case "soft":
                        softDelete();
                        break;
                    case "redo":
                        redoDelete();
                        break;
                    case "hard":
                        hardDelete();
                        break;
                    default:
                        throw new ParameterException("delete option must in { soft | redo | hard }");
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

    }

}
