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
import com.github.pagehelper.PageInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.inner.client.InlongGroupClient;
import org.apache.inlong.manager.client.cli.pojo.GroupInfo;
import org.apache.inlong.manager.client.cli.util.ClientUtils;
import org.apache.inlong.manager.client.cli.util.PrintUtils;
import org.apache.inlong.manager.pojo.group.InlongGroupBriefInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupPageRequest;

import java.util.List;

/**
 * The log command used to get filter certain kinds of inlong groups
 */
@Parameters(commandDescription = "Log resource")
public class LogCommand extends AbstractCommand {

    @Parameter()
    private List<String> params;

    public LogCommand() {
        super("log");
        jcommander.addCommand("group", new CreateGroup());
    }

    @Parameters(commandDescription = "Log group")
    private static class CreateGroup extends AbstractCommandRunner {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"--query"}, description = "condition filters")
        private String input;

        @Override
        void run() {
            final int MAX_LOG_SIZE = 100;
            try {
                // for now only filter by one condition. TODO:support OR and AND, make a condition filter.
                //sample input: inlongGroupId:test_group
                String[] inputs = input.split(":");
                ClientUtils.initClientFactory();
                InlongGroupClient groupClient = ClientUtils.clientFactory.getGroupClient();
                InlongGroupPageRequest pageRequest = new InlongGroupPageRequest();
                if(StringUtils.isNotBlank(inputs[1])) {
                    pageRequest.setKeyword(inputs[1]);
                }
                PageInfo<InlongGroupBriefInfo> pageInfo = groupClient.listGroups(pageRequest);
                if (pageInfo.getSize() > MAX_LOG_SIZE) {
                    System.err.println("log too large to print, consider changing filter.");
                    return;
                }
                PrintUtils.print(pageInfo.getList(), GroupInfo.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
