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

import org.apache.inlong.tubemq.corebase.utils.TStringUtils;
import org.apache.inlong.tubemq.server.common.fielddef.CliArgDef;
import org.apache.inlong.tubemq.server.common.utils.HttpUtils;

import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is use to process Web Api Request process.
 */
public class CliWebapiAdmin extends CliAbstractBase {

    private static final Logger logger =
            LoggerFactory.getLogger(CliBrokerAdmin.class);

    private static final String defMasterPortal = "127.0.0.1:8080";

    private Map<String, Object> requestParams;

    public CliWebapiAdmin() {
        super(null);
        initCommandOptions();
    }

    /**
     * Construct CliWebapiAdmin with request parameters
     *
     * @param requestParams Request parameters map
     */
    public CliWebapiAdmin(Map<String, Object> requestParams) {
        this();
        this.requestParams = requestParams;
    }

    /**
     * Init command options
     */
    @Override
    protected void initCommandOptions() {
        // add the cli required parameters
        addCommandOption(CliArgDef.MASTERPORTAL);
        addCommandOption(CliArgDef.ADMINMETHOD);
        addCommandOption(CliArgDef.METHOD);
    }

    /**
     * Call the Web API
     *
     * @param args Request parameters of method name,
     *             {"--method", "admin_query_topic_info"} as an example
     */
    @Override
    public boolean processParams(String[] args) throws Exception {
        CommandLine cli = parser.parse(options, args);
        if (cli == null) {
            throw new ParseException("Parse args failure");
        }
        if (cli.hasOption(CliArgDef.VERSION.longOpt)) {
            version();
        }
        if (cli.hasOption(CliArgDef.HELP.longOpt)) {
            help();
        }
        String masterAddr = defMasterPortal;
        if (cli.hasOption(CliArgDef.MASTERPORTAL.longOpt)) {
            masterAddr = cli.getOptionValue(CliArgDef.MASTERPORTAL.longOpt);
            if (TStringUtils.isBlank(masterAddr)) {
                throw new Exception(CliArgDef.MASTERPORTAL.longOpt + " is required!");
            }
        }
        JsonObject result = null;
        String masterUrl = "http://" + masterAddr + "/webapi.htm";
        if (cli.hasOption(CliArgDef.ADMINMETHOD.longOpt)) {
            Map<String, String> inParamMap = new HashMap<>();
            inParamMap.put(CliArgDef.METHOD.longOpt, "admin_get_methods");
            result = HttpUtils.requestWebService(masterUrl, inParamMap);
            System.out.println(formatResult(result));
            System.exit(0);
        }
        String methodStr = cli.getOptionValue(CliArgDef.METHOD.longOpt);
        if (TStringUtils.isBlank(methodStr)) {
            throw new Exception(CliArgDef.METHOD.longOpt + " is required!");
        }
        requestParams.put(CliArgDef.METHOD.longOpt, methodStr);
        Map<String, String> convertedRequestParams = convertRequestParams(requestParams);
        result = HttpUtils.requestWebService(masterUrl, convertedRequestParams);
        String formattedResult = formatResult(result);
        System.out.println(formattedResult);
        return true;
    }

    /**
     * Convert request paramters map
     *
     * @param requestParamsMap Map
     * @return a converted map
     */
    private Map<String, String> convertRequestParams(Map<String, Object> requestParamsMap) {
        // convert object values to string ones
        Map<String, String> converttedrequestParamsMap = new HashMap<>();
        for (String k : requestParamsMap.keySet()) {
            converttedrequestParamsMap.put(k, String.valueOf(requestParamsMap.get(k)));
        }
        return converttedrequestParamsMap;
    }

    /**
     * Convert json content to specific output format
     *
     * @param result JsonObject
     * @return formatted results
     */
    private String formatResult(JsonObject result) {
        // format output results
        return new GsonBuilder().setPrettyPrinting().create().toJson(result);
    }

    public static void main(String[] args) {
        CliWebapiAdmin cliWebapiAdmin = new CliWebapiAdmin();
        try {
            cliWebapiAdmin.processParams(args);
        } catch (Throwable ex) {
            ex.printStackTrace();
            logger.error(ex.getMessage());
            cliWebapiAdmin.help();
        }
    }
}
