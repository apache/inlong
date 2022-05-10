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
import org.apache.commons.collections.CollectionUtils;
import org.apache.inlong.manager.client.api.InlongGroupContext.InlongGroupStatus;
import org.apache.inlong.manager.client.api.inner.InnerInlongManagerClient;
import org.apache.inlong.manager.client.cli.pojo.GroupInfo;
import org.apache.inlong.manager.client.cli.pojo.SinkInfo;
import org.apache.inlong.manager.client.cli.pojo.SourceInfo;
import org.apache.inlong.manager.client.cli.pojo.StreamInfo;
import org.apache.inlong.manager.client.cli.util.PrintUtils;
import org.apache.inlong.manager.common.beans.Response;
import org.apache.inlong.manager.common.pojo.group.InlongGroupListResponse;
import org.apache.inlong.manager.common.pojo.group.InlongGroupPageRequest;
import org.apache.inlong.manager.common.pojo.sink.SinkListResponse;
import org.apache.inlong.manager.common.pojo.source.SourceListResponse;
import org.apache.inlong.manager.common.pojo.stream.FullStreamResponse;
import org.apache.inlong.manager.common.pojo.stream.InlongStreamInfo;

import java.util.ArrayList;
import java.util.List;

@Parameters(commandDescription = "Displays main information for one or more resources")
public class CommandList extends CommandBase {

    @Parameter()
    private List<String> params;

    public CommandList() {
        super("list");
        jcommander.addCommand("stream", new ListStream());
        jcommander.addCommand("group", new ListGroup());
        jcommander.addCommand("sink", new ListSink());
        jcommander.addCommand("source", new ListSource());
    }

    @Parameters(commandDescription = "Get stream main information")
    private static class ListStream extends CommandUtil {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-g", "--group"}, required = true, description = "inlong group id")
        private String groupId;

        @Override
        void run() {
            InnerInlongManagerClient managerClient = new InnerInlongManagerClient(connect().getConfiguration());
            try {
                List<FullStreamResponse> fullStreamResponseList = managerClient.listStreamInfo(groupId);
                List<InlongStreamInfo> inlongStreamInfoList = new ArrayList<>();
                fullStreamResponseList.forEach(fullStreamResponse -> {
                    inlongStreamInfoList.add(fullStreamResponse.getStreamInfo());
                });
                PrintUtils.print(inlongStreamInfoList, StreamInfo.class);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Get group details")
    private static class ListGroup extends CommandUtil {

        private static final int DEFAULT_PAGE_SIZE = 10;

        @Parameter()
        private List<String> params;
        @Parameter(names = {"-s", "--status"})
        private String status;
        @Parameter(names = {"-g", "--group"}, description = "inlong group id")
        private String group;
        @Parameter(names = {"-n", "--num"}, description = "the number displayed")
        private int pageSize;

        @Override
        void run() {
            try {
                InlongGroupPageRequest pageRequest = new InlongGroupPageRequest();
                pageRequest.setKeyword(group);
                pageSize = pageSize <= 0 ? DEFAULT_PAGE_SIZE : pageSize;
                pageRequest.setPageNum(1).setPageSize(pageSize);

                List<Integer> statusList = null;
                if (status != null) {
                    statusList = InlongGroupStatus.parseStatusCodeByStr(status);
                }
                if (CollectionUtils.isEmpty(statusList)) {
                    statusList = InlongGroupStatus.parseStatusCodeByStr(InlongGroupStatus.STARTED.toString());
                }
                pageRequest.setStatusList(statusList);

                InnerInlongManagerClient client = new InnerInlongManagerClient(connect().getConfiguration());
                Response<PageInfo<InlongGroupListResponse>> pageInfoResponse = client.listGroups(pageRequest);
                List<InlongGroupListResponse> groupList = pageInfoResponse.getData().getList();

                PrintUtils.print(groupList, GroupInfo.class);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Get sink details")
    private static class ListSink extends CommandUtil {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-s", "--stream"}, required = true, description = "stream id")
        private String stream;

        @Parameter(names = {"-g", "--group"}, required = true, description = "group id")
        private String group;

        @Override
        void run() {
            InnerInlongManagerClient managerClient = new InnerInlongManagerClient(connect().getConfiguration());
            try {
                List<SinkListResponse> sinkListResponses = managerClient.listSinks(group, stream);
                PrintUtils.print(sinkListResponses, SinkInfo.class);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    @Parameters(commandDescription = "Get source details")
    private static class ListSource extends CommandUtil {

        @Parameter()
        private List<String> params;

        @Parameter(names = {"-s", "--stream"}, required = true, description = "inlong stream id")
        private String stream;

        @Parameter(names = {"-g", "--group"}, required = true, description = "inlong group id")
        private String group;

        @Parameter(names = {"-t", "--type"}, description = "sink type")
        private String type;

        @Override
        void run() {
            InnerInlongManagerClient managerClient = new InnerInlongManagerClient(connect().getConfiguration());
            try {
                List<SourceListResponse> sourceListResponses = managerClient.listSources(group, stream, type);
                PrintUtils.print(sourceListResponses, SourceInfo.class);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
