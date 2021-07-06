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

package org.apache.inlong.tubemq.server.master.web.action.screen;

import static org.apache.inlong.tubemq.server.common.webbase.WebMethodMapper.getWebApiRegInfo;

import java.util.Arrays;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.inlong.tubemq.corebase.TBaseConstants;
import org.apache.inlong.tubemq.corebase.utils.TStringUtils;
import org.apache.inlong.tubemq.corerpc.exception.StandbyException;
import org.apache.inlong.tubemq.server.common.webbase.WebMethodMapper;
import org.apache.inlong.tubemq.server.master.TMaster;
import org.apache.inlong.tubemq.server.master.nodemanage.nodebroker.BrokerConfManager;
import org.apache.inlong.tubemq.server.master.web.handler.AbstractWebHandler;
import org.apache.inlong.tubemq.server.master.web.handler.WebAdminFlowRuleHandler;
import org.apache.inlong.tubemq.server.master.web.handler.WebAdminGroupCtrlHandler;
import org.apache.inlong.tubemq.server.master.web.handler.WebAdminTopicAuthHandler;
import org.apache.inlong.tubemq.server.master.web.handler.WebBrokerDefConfHandler;
import org.apache.inlong.tubemq.server.master.web.handler.WebBrokerTopicConfHandler;
import org.apache.inlong.tubemq.server.master.web.handler.WebMasterInfoHandler;
import org.apache.inlong.tubemq.server.master.web.handler.WebOtherInfoHandler;
import org.apache.inlong.tubemq.server.master.web.simplemvc.Action;
import org.apache.inlong.tubemq.server.master.web.simplemvc.RequestContext;


/**
 * Public APIs for master
 *
 * Encapsulate query and modify logic mainly.
 * Generate output JSON by concatenating strings, to improve the performance.
 */
public class Webapi implements Action {
    // allowed type value set
    private static final List<String> allowedTypeValues =
            Arrays.asList("op_query", "op_modify");
    private TMaster master;


    public Webapi(TMaster master) {
        this.master = master;
        registerHandler(new WebBrokerDefConfHandler(this.master));
        registerHandler(new WebBrokerTopicConfHandler(this.master));
        registerHandler(new WebAdminGroupCtrlHandler(this.master));
        registerHandler(new WebAdminTopicAuthHandler(this.master));
        registerHandler(new WebAdminFlowRuleHandler(this.master));
        registerHandler(new WebMasterInfoHandler(this.master));
        registerHandler(new WebOtherInfoHandler(this.master));
    }

    @Override
    public void execute(RequestContext requestContext) {
        StringBuilder strBuffer = new StringBuilder();
        try {
            HttpServletRequest req = requestContext.getReq();
            if (this.master.isStopped()) {
                throw new Exception("Server is stopping...");
            }
            BrokerConfManager brokerConfManager = this.master.getMasterTopicManager();
            if (!brokerConfManager.isSelfMaster()) {
                throw new StandbyException("Please send your request to the master Node.");
            }
            String type = req.getParameter("type");
            String method = req.getParameter("method");
            String strCallbackFun = req.getParameter("callback");
            if ((TStringUtils.isNotEmpty(strCallbackFun))
                    && (strCallbackFun.length() <= TBaseConstants.META_MAX_CALLBACK_STRING_LENGTH)
                    && (strCallbackFun.matches(TBaseConstants.META_TMP_CALLBACK_STRING_VALUE))) {
                strCallbackFun = strCallbackFun.trim();
            }
            if (type == null) {
                throw new Exception("Please take with type parameter!");
            }
            if (method == null) {
                throw new Exception("Please take with method parameter!");
            }
            if (!allowedTypeValues.contains(type)) {
                throw new Exception("Unsupported operate type, only support " + allowedTypeValues);
            }
            boolean isQuery = true;
            if ("op_modify".equals(type)) {
                isQuery = false;
                // check master is current node
                if (brokerConfManager.isPrimaryNodeActive()) {
                    throw new Exception(
                            "DesignatedPrimary happened...please check if the other member is down");
                }
            }
            WebMethodMapper.WebApiRegInfo webApiRegInfo = getWebApiRegInfo(method);
            if (webApiRegInfo == null) {
                strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"Unsupported method: ")
                        .append(method).append("\"}");
                requestContext.put("sb", strBuffer.toString());
            } else {

                strBuffer = (StringBuilder) webApiRegInfo.method.invoke(webApiRegInfo.webHandler, req);
                // Carry callback information
                if (TStringUtils.isEmpty(strCallbackFun)) {
                    requestContext.put("sb", strBuffer.toString());
                } else {
                    requestContext.put("sb", strCallbackFun + "(" + strBuffer.toString() + ")");
                    requestContext.getResp().addHeader("Content-type", "text/plain");
                    requestContext.getResp().addHeader("charset", TBaseConstants.META_DEFAULT_CHARSET_NAME);
                }
            }
        } catch (Throwable e) {
            strBuffer.append("{\"result\":false,\"errCode\":400,\"errMsg\":\"Bad request from client, ")
                    .append(e.getMessage()).append("\"}");
            requestContext.put("sb", strBuffer.toString());
        }
    }

    private void registerHandler(AbstractWebHandler abstractWebHandler) {
        abstractWebHandler.registerWebApiMethod();
    }

}
