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

package org.apache.inlong.dataproxy.http;

import static org.apache.inlong.dataproxy.consts.AttributeConstants.GROUP_ID;
import static org.apache.inlong.dataproxy.consts.AttributeConstants.DATA_TIME;
import static org.apache.inlong.dataproxy.consts.AttributeConstants.STREAM_ID;
import static org.apache.inlong.dataproxy.http.HttpSourceConstants.BODY;
import static org.apache.inlong.dataproxy.http.HttpSourceConstants.HTTP_REQUEST;
import static org.apache.inlong.dataproxy.http.HttpSourceConstants.HTTP_RESPONSE;

import org.apache.inlong.common.monitor.LogCounter;
import org.apache.inlong.dataproxy.http.exception.MessageProcessException;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessServlet
        extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(MessageProcessServlet.class);
    private static final LogCounter logCounter = new LogCounter(10, 100000, 60 * 1000);

    private MessageHandler messageHandler;

    public MessageProcessServlet(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        try {
            Context context = new MappedContext();

            context.put(GROUP_ID, req.getParameter(GROUP_ID));
            context.put(STREAM_ID, req.getParameter(STREAM_ID));
            context.put(DATA_TIME, req.getParameter(DATA_TIME));
            context.put(BODY, req.getParameter(BODY));

            context.put(HTTP_REQUEST, req);
            context.put(HTTP_RESPONSE, resp);

            messageHandler.processMessage(context);
        } catch (MessageProcessException e) {
            if (logCounter.shouldPrint()) {
                LOG.error("Received bad request from client. ", e);
            }
            req.setAttribute("code", StatusCode.SERVICE_ERR);
        }
    }
}
