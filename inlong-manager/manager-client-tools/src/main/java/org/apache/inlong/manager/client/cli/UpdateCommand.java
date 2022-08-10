
package org.apache.inlong.manager.client.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FileConverter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.client.api.InlongClient;
import org.apache.inlong.manager.client.api.InlongGroup;
import org.apache.inlong.manager.client.api.InlongGroupContext;
import org.apache.inlong.manager.client.api.InlongStreamBuilder;
import org.apache.inlong.manager.client.cli.pojo.CreateGroupConf;
import org.apache.inlong.manager.client.cli.util.ClientUtils;
import org.apache.inlong.manager.pojo.sort.BaseSortConf;

import java.io.File;

@Parameters(commandDescription = "Update resource by json file")
public class UpdateCommand extends AbstractCommand {
    @Parameter()
    private java.util.List<String> params;

    public UpdateCommand() {
        super("update");
        jcommander.addCommand("update", new UpdateCommand.UpdateGroup());
    }

    @Parameters(commandDescription = "Update group by json file")
    private static class UpdateGroup extends AbstractCommandRunner {

        @Parameter()
        private java.util.List<String> params;

        @Parameter(names = {"-id"},
                required = true)
        private String groupId;

        @Parameter(names = {"-c", "--config"},
                converter = FileConverter.class,
                required = true,
                description = "json file")
        private File file;

        @Override
        void run() {
            try {
                InlongClient inlongClient = ClientUtils.getClient();
                InlongGroup group = inlongClient.getGroup(groupId);
                String fileContent = ClientUtils.readFile(file);
                if (StringUtils.isBlank(fileContent)) {
                    System.out.println("Create group failed: file was empty!");
                    return;
                }
                //first extract groupconfig from the file passed in
                BaseSortConf sortConf = objectMapper.readValue(fileContent, BaseSortConf.class);
                group.update(sortConf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
