/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.agent.core.conf;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.tubemq.agent.conf.JobProfile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigServlet extends HttpServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigServlet.class);

    private static final String CONTENT_TYPE = "application/json";
    private static final String CHARSET_TYPE = "UTF-8";
    private final Gson gson = new Gson();
    private final ConfigJetty configCenter;

    public ConfigServlet(ConfigJetty configCenter) {
        this.configCenter = configCenter;
    }

    public void responseToJson(HttpServletResponse response,
        ResponseResult result) throws IOException {
        response.setContentType(CONTENT_TYPE);
        response.setCharacterEncoding(CHARSET_TYPE);
        String jsonStr = gson.toJson(result);
        PrintWriter out = response.getWriter();
        out.print(jsonStr);
        out.flush();
    }

    /**
     * handle post requests.
     *
     * @param req  - request
     * @param resp - response
     */
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String pathInfo = req.getPathInfo();
        ResponseResult responseResult = new ResponseResult(0, "");

        try (BufferedReader reader = req.getReader()) {
            String configJsonStr = IOUtils.toString(reader);
            if (pathInfo.endsWith("job")) {
                JobProfile jobConfiguration = JobProfile.parseJsonStr(configJsonStr);
                configCenter.storeJobConf(jobConfiguration);
            } else if (pathInfo.endsWith("agent")) {
                // TODO add new agent configuration
                configCenter.storeAgentConf(configJsonStr);
            } else {
                responseResult.setCode(-1).setMessage("child path is not correct");
            }
        } catch (Exception ex) {
            LOGGER.error("error while handle post", ex);
            responseResult.setCode(-1).setMessage(ex.getMessage());
        }
        responseToJson(resp, responseResult);
    }
}
