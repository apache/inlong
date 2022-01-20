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

package org.apache.inlong.dataproxy.metrics;

import com.google.gson.Gson;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.inlong.commons.util.MetricUtil;
import org.apache.inlong.commons.metrics.ChannelMetric;
import org.apache.inlong.commons.metrics.ChannelMetricResult;
import org.apache.inlong.dataproxy.config.remote.ResponseResult;
import org.apache.inlong.dataproxy.http.StatusCode;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WaterMarkServlet
        extends HttpServlet {

    private static final Logger LOG = LoggerFactory.getLogger(WaterMarkServlet.class);
    private static final ConcurrentHashMap<Channel, ChannelMetric> waterMarkMetric =
            new ConcurrentHashMap<>();
    private final Gson gson = new Gson();

    public WaterMarkServlet() {
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doPost(req, resp);
    }

    private void responseToJson(HttpServletResponse response, ResponseResult result) {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        String jsonStr = gson.toJson(result);
        try (PrintWriter out = response.getWriter()) {
            out.print(jsonStr);
            out.flush();
        } catch (Exception e) {
            LOG.error("close writer exception", e);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        ResponseResult result = new ResponseResult(StatusCode.SERVICE_ERR, "");
        try {
            String metricResult = getMetricResult(waterMarkMetric);
            result.setMessage(metricResult);
            result.setCode(StatusCode.SUCCESS);
        } catch (Exception ex) {
            LOG.error("error while do post", ex);
            result.setMessage(ex.getMessage());
        }
        responseToJson(resp, result);
    }

    private String getMetricResult(ConcurrentHashMap<Channel, ChannelMetric> waterMarkMetric) {
        AtomicInteger highCount = new AtomicInteger();
        AtomicInteger middleCount = new AtomicInteger();
        AtomicLong currentBufferSize = new AtomicLong();
        calculateMetric(waterMarkMetric, highCount, middleCount, currentBufferSize);
        ChannelMetricResult result = new ChannelMetricResult();
        result.setChannelHighWaterMarkCount(highCount.get());
        result.setChannelMiddleWaterMarkCount(middleCount.get());
        result.setChannelCount(waterMarkMetric.size());
        result.setMemoryUsage(MetricUtil.getHeapMemLoadInfo());
        result.setOverAllBufferSize(currentBufferSize.get());
        if (!waterMarkMetric.isEmpty()) {
            result.setAverageBufferSize((int) (currentBufferSize.get() / waterMarkMetric.size()));
        }
        return gson.toJson(result);
    }

    private void calculateMetric(ConcurrentHashMap<Channel, ChannelMetric> waterMarkMetric,
            AtomicInteger highCount, AtomicInteger middleCount, AtomicLong currentBufferSize) {
        waterMarkMetric.forEach((channel, channelMetric) -> {
            ChannelMetric metric = (ChannelMetric) channel.getAttachment();
            if (metric.isOverHighWaterMark()) {
                highCount.addAndGet(1);
            }
            if (metric.isOverMiddleWaterMark()) {
                middleCount.addAndGet(1);
            }
            currentBufferSize.addAndGet(metric.getCurrentBufferSize());
        });
    }
}

