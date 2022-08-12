package org.apache.inlong.manager.client.cli;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import com.github.pagehelper.PageInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.api.inner.client.InlongGroupClient;
import org.apache.inlong.manager.client.cli.pojo.CreateGroupConf;
import org.apache.inlong.manager.client.cli.pojo.GroupInfo;
import org.apache.inlong.manager.client.cli.util.ClientUtils;
import org.apache.inlong.manager.client.cli.util.PrintUtils;
import org.apache.inlong.manager.pojo.group.InlongGroupBriefInfo;
import org.apache.inlong.manager.pojo.group.InlongGroupPageRequest;

import java.io.File;

/**
 * Log resource
 */
@Parameters(commandDescription = "Log resource")
public class LogCommand extends AbstractCommand {

    @Parameter()
    private java.util.List<String> params;

    public LogCommand() {
        super("log");
        jcommander.addCommand("group", new CreateGroup());
    }

    @Parameters(commandDescription = "Log group")
    private static class CreateGroup extends AbstractCommandRunner {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"--query"})
        private String input;

        @Override
        void run() {
            final int MAX_LOG_SIZE = 100;
            //for now only filter by one condition. TODO:support OR and AND, make a condition filter.
            String[] inputs =  input.split(":");
            ClientUtils.initClientFactory();
            InlongGroupClient groupClient = ClientUtils.clientFactory.getGroupClient();
            InlongGroupPageRequest pageRequest = new InlongGroupPageRequest();
            pageRequest.setKeyword(inputs[1]);
            PageInfo<InlongGroupBriefInfo> pageInfo = groupClient.listGroups(pageRequest);
            if(pageInfo.getSize()>MAX_LOG_SIZE){
                System.err.println("log too large to print, consider changing filter.");
                return;
            }
            PrintUtils.print(pageInfo.getList(), GroupInfo.class);
        }
    }
}