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

package org.apache.inlong.manager.web.trace;

import org.apache.inlong.manager.common.util.JsonUtils;
import org.apache.inlong.manager.web.utils.InlongRequestWrapper;

import com.fasterxml.jackson.databind.JsonNode;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.shiro.web.servlet.ShiroHttpServletRequest;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

@Aspect
@Component
@Slf4j
public class TraceFilter implements Filter {

    private static final String INLONG_GROUP_ID = "inlongGroupId";
    private static final String CLUSTER_NAME = "clusterName";
    private static final String CLUSTER_TAG = "clusterTag";
    private static final String INLONG_STREAM = "inlongStreamId";
    private static final String TRACE_ID = "trace-id";
    private static final String REQUEST = "request";
    private static final String BODY = "body";
    private static final String URI = "URI";
    private static final String RESPONSE = "response";

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain)
            throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        InlongRequestWrapper inlongRequestWrapper;
        if (request instanceof ShiroHttpServletRequest) {
            ShiroHttpServletRequest shiroWrapper = (ShiroHttpServletRequest) request;
            inlongRequestWrapper = (InlongRequestWrapper) shiroWrapper.getRequest();
        } else if (request instanceof InlongRequestWrapper) {
            inlongRequestWrapper = (InlongRequestWrapper) request;
        } else {
            String errMsg = "request should be one of ShiroHttpServletRequest or InlongRequestWrapper type";
            log.error(errMsg);
            response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, errMsg);
            return;
        }
        ResponseWrapper responseWrapper = new ResponseWrapper(response);
        Span span = Span.current();
        addAttributes(inlongRequestWrapper.getContent(), request);
        span.addEvent(REQUEST, Attributes.builder().put(BODY, inlongRequestWrapper.getContent())
                .put(URI, request.getRequestURI()).build());
        response.addHeader(TRACE_ID, span.getSpanContext().getTraceId());
        filterChain.doFilter(inlongRequestWrapper, responseWrapper);
        span.addEvent(RESPONSE, Attributes.builder().put(BODY, responseWrapper.getContent()).build());
    }

    private void addAttributes(String body, HttpServletRequest request) {
        try {
            JsonNode jsonNode = JsonUtils.parseTree(body);
            addParamInBody(jsonNode, INLONG_GROUP_ID);
            addParamInBody(jsonNode, CLUSTER_NAME);
            addParamInBody(jsonNode, CLUSTER_TAG);
            addParamInBody(jsonNode, INLONG_STREAM);
            addParamInURI(request, INLONG_GROUP_ID);
            addParamInURI(request, CLUSTER_NAME);
            addParamInURI(request, CLUSTER_TAG);
            addParamInURI(request, INLONG_STREAM);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void addParamInBody(JsonNode jsonNode, String paramName) {
        JsonNode nodeValue = jsonNode.get(paramName);
        if (nodeValue != null) {
            Span.current().setAttribute(paramName, nodeValue.asText());
        }
    }

    private void addParamInURI(HttpServletRequest request, String paramName) {
        String param = request.getParameter(paramName);
        if (StringUtils.isNotBlank(param)) {
            Span.current().setAttribute(paramName, param);
        }
    }
}
