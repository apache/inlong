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
package org.apache.inlong.manager.client.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupContext;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.inner.client.InlongGroupClient;
import org.apache.inlong.manager.client.cli.pojo.CreateGroupConf;
import org.apache.inlong.manager.client.cli.util.ClientUtils;
import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.client.cli.CommandToolMain.*;

import java.io.File;

@Parameters(commandDescription = "Delete resource by json file")
public class DeleteCommand extends AbstractCommand {
    @Parameter()
    private java.util.List<String> params;

    public DeleteCommand() {
        super("delete");
        jcommander.addCommand("group", new DeleteCommand.DeleteGroup());
    }

    @Parameters(commandDescription = "Delete group by group id")
    private static class DeleteGroup extends AbstractCommandRunner {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"--group","-g"},
                required = true,
                description = "group id")
        private String groupId;

        @Override
        void run() {
            //get the group and the corresponding context(snapshot)
            try{
                InlongClient inlongClient = ClientUtils.getClient();
                InlongGroup group = inlongClient.getGroup(groupId);
                group.delete();
                System.out.println("delete group success");
            }
            //TODO: handle and/or classify the exceptions
            catch(Exception e){
                System.out.format("Delete group failed! message: %s \n",e.getMessage());
            }
        }
    }
}
