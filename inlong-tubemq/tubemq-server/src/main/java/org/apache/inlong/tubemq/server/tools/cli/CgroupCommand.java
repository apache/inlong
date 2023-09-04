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
import com.beust.jcommander.Parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

<<<<<<< HEAD
<<<<<<< HEAD
=======
/**
 * @author zfancy
 * @version 1.0
 */
>>>>>>> Add command line tool: tubectl, and its documents.
=======
>>>>>>> Add unit tests and update documents.
@Parameters(commandDescription = "Consumer group commands")
public class CgroupCommand extends AbstractCommand {

    @Parameter()
    private List<String> params;

    final private static String[] requestMethod = new String[]{"--method", ""};
    final private static Map<String, Object> requestParams = new HashMap<>();
    final private static CliWebapiAdmin cliWebapiAdmin = new CliWebapiAdmin(requestParams);

    public CgroupCommand() {
        super("cgroup");

        jcommander.addCommand("list", new CgroupList());
        jcommander.addCommand("create", new CgroupCreate());
        jcommander.addCommand("delete", new CgroupDelete());
    }

    @Parameters(commandDescription = "Consumer group List")
    private static class CgroupList extends AbstractCommandRunner {

        @Parameter()
        private List<String> params = new ArrayList<>();

        @Parameter(names = {"-n", "--topicName"}, order = 0, description = "String. Topic name")
        private String topicName;

        @Parameter(names = {"-g", "--groupName"}, order = 1, description = "String. Consumer group name")
        private String groupName;

        @Parameter(names = {"-c", "--createUser"}, order = 3, description = "String. Record creator")
        private String createUser;

        @Override
        void run() {
            try {
                requestMethod[1] = "admin_query_allowed_consumer_group_info";
                requestParams.clear();
                if (topicName != null)
                    requestParams.put(WebFieldDef.TOPICNAME.name, topicName);
                if (groupName != null)
                    requestParams.put(WebFieldDef.GROUPNAME.name, groupName);
                if (createUser != null)
                    requestParams.put(WebFieldDef.CREATEUSER.name, createUser);
                cliWebapiAdmin.processParams(requestMethod);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Consumer group Create")
    private static class CgroupCreate extends AbstractCommandRunner {

        @Parameter()
        private List<String> params = new ArrayList<>();

        @Parameter(names = {"-n", "--topicName"}, order = 0, required = true, description = "String. Topic name")
        private String topicName;

        @Parameter(names = {"-g",
                "--groupName"}, order = 1, required = true, description = "String. Consumer group name")
        private String groupName;

        @Parameter(names = {"-at",
                "--confModAuthToken"}, order = 2, required = true, description = "String. Admin api operation authorization code")
        private String confModAuthToken;

        @Parameter(names = {"-c", "--createUser"}, order = 3, required = true, description = "String. Record creator")
        private String createUser;

        @Parameter(names = {"-cd", "--createDate"}, order = 4, description = "String. Record creation date")
        private String createDate;

        @Override
        void run() {
            try {
                requestMethod[1] = "admin_add_authorized_consumergroup_info";
                requestParams.clear();
                if (topicName != null)
                    requestParams.put(WebFieldDef.TOPICNAME.name, topicName);
                if (groupName != null)
                    requestParams.put(WebFieldDef.GROUPNAME.name, groupName);
                if (confModAuthToken != null)
                    requestParams.put(WebFieldDef.ADMINAUTHTOKEN.name, confModAuthToken);
                if (createUser != null)
                    requestParams.put(WebFieldDef.CREATEUSER.name, createUser);
                if (createDate != null)
                    requestParams.put(WebFieldDef.CREATEDATE.name, createDate);
                cliWebapiAdmin.processParams(requestMethod);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Consumer group Delete")
    private static class CgroupDelete extends AbstractCommandRunner {

        @Parameter()
        private List<String> params = new ArrayList<>();

        @Parameter(names = {"-n", "--topicName"}, order = 0, required = true, description = "String. Topic name")
        private String topicName;

        @Parameter(names = {"-at",
                "--confModAuthToken"}, order = 1, required = true, description = "String. Admin api operation authorization code")
        private String confModAuthToken;

<<<<<<< HEAD
<<<<<<< HEAD
        @Parameter(names = {"-m", "--modifyUser"}, required = true, order = 13, description = "String. Record modifier")
        private String modifyUser;

=======
>>>>>>> Add command line tool: tubectl, and its documents.
=======
        @Parameter(names = {"-m", "--modifyUser"}, required = true, order = 13, description = "String. Record modifier")
        private String modifyUser;

>>>>>>> Add unit tests and update documents.
        @Parameter(names = {"-g", "--groupName"}, order = 2, description = "String. Consumer group name")
        private String groupName;

        @Override
        void run() {
            try {
                requestMethod[1] = "admin_delete_allowed_consumer_group_info";
                requestParams.clear();
                if (topicName != null)
                    requestParams.put(WebFieldDef.TOPICNAME.name, topicName);
                if (confModAuthToken != null)
                    requestParams.put(WebFieldDef.ADMINAUTHTOKEN.name, confModAuthToken);
<<<<<<< HEAD
<<<<<<< HEAD
                if (modifyUser != null)
                    requestParams.put(WebFieldDef.MODIFYUSER.name, modifyUser);
=======
>>>>>>> Add command line tool: tubectl, and its documents.
=======
                if (modifyUser != null)
                    requestParams.put(WebFieldDef.MODIFYUSER.name, modifyUser);
>>>>>>> Add unit tests and update documents.
                if (groupName != null)
                    requestParams.put(WebFieldDef.GROUPNAME.name, groupName);
                cliWebapiAdmin.processParams(requestMethod);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
