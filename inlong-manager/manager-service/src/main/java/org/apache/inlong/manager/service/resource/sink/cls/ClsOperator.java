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

package org.apache.inlong.manager.service.resource.sink.cls;

import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.exceptions.BusinessException;

import com.tencentcloudapi.cls.v20201016.ClsClient;
import com.tencentcloudapi.cls.v20201016.models.CreateIndexRequest;
import com.tencentcloudapi.cls.v20201016.models.CreateIndexResponse;
import com.tencentcloudapi.cls.v20201016.models.CreateTopicRequest;
import com.tencentcloudapi.cls.v20201016.models.CreateTopicResponse;
import com.tencentcloudapi.cls.v20201016.models.DescribeIndexRequest;
import com.tencentcloudapi.cls.v20201016.models.DescribeIndexResponse;
import com.tencentcloudapi.cls.v20201016.models.DescribeTopicsRequest;
import com.tencentcloudapi.cls.v20201016.models.DescribeTopicsResponse;
import com.tencentcloudapi.cls.v20201016.models.Filter;
import com.tencentcloudapi.cls.v20201016.models.FullTextInfo;
import com.tencentcloudapi.cls.v20201016.models.ModifyIndexRequest;
import com.tencentcloudapi.cls.v20201016.models.ModifyIndexResponse;
import com.tencentcloudapi.cls.v20201016.models.ModifyTopicRequest;
import com.tencentcloudapi.cls.v20201016.models.ModifyTopicResponse;
import com.tencentcloudapi.cls.v20201016.models.RuleInfo;
import com.tencentcloudapi.cls.v20201016.models.Tag;
import com.tencentcloudapi.cls.v20201016.models.TopicInfo;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ClsOperator {

    @Value("${cls.manager.endpoint}")
    private String endpoint;
    private static final Logger LOG = LoggerFactory.getLogger(ClsOperator.class);
    private static final String TOPIC_ID = "topicId";
    private static final String LOG_SET_ID = "logsetId";
    private static final long PRECISE_SEARCH = 1L;

    public String createTopicReturnTopicId(String topicName, String logSetId, String tag, Integer storageDuration,
            String secretId, String secretKey, String region)
            throws Exception {
        ClsClient client = getClsClient(secretId, secretKey, region);
        CreateTopicRequest req = getCreateTopicRequest(tag, logSetId, topicName, storageDuration);
        CreateTopicResponse resp = client.CreateTopic(req);
        LOG.info("create cls topic success for topicName = {}, topicId = {}, requestId = {}", topicName,
                resp.getTopicId(), resp.getRequestId());
        updateTopicTag(resp.getTopicId(), tag, secretId, secretKey, region);
        return resp.getTopicId();
    }

    public void updateTopicTag(String topicId, String tag, String secretId, String secretKey, String region)
            throws Exception {
        ClsClient client = getClsClient(secretId, secretKey, region);
        ModifyTopicRequest modifyTopicRequest = new ModifyTopicRequest();
        modifyTopicRequest.setTags(convertTags(tag.split(InlongConstants.CENTER_LINE)));
        modifyTopicRequest.setTopicId(topicId);
        ModifyTopicResponse resp = client.ModifyTopic(modifyTopicRequest);
        LOG.info("update cls topic tag success for topicId = {}, requestId = {}", topicId, resp.getRequestId());
    }

    /**
     * Create topic index by tokenizer
     */
    public void createTopicIndex(String tokenizer, String topicId, String secretId, String secretKey, String region)
            throws BusinessException {

        LOG.debug("create topic index start for topicId = {}, tokenizer = {}", topicId, tokenizer);
        if (StringUtils.isBlank(tokenizer)) {
            LOG.warn("tokenizer is blank for topic = {}", topicId);
            return;
        }
        FullTextInfo topicIndexFullText = getTopicIndexFullText(secretId, secretKey, region, topicId);
        if (ObjectUtils.anyNotNull(topicIndexFullText)) {
            // if topic index exist, update
            LOG.debug("cls topic is exist and update for topicId = {},tokenizer = {}", topicId, tokenizer);
            updateTopicIndex(tokenizer, topicId, secretId, secretKey, region);
            return;
        }
        ClsClient clsClient = getClsClient(secretId, secretKey, region);
        CreateIndexRequest req = getCreateIndexRequest(tokenizer, topicId);
        try {
            CreateIndexResponse createIndexResponse = clsClient.CreateIndex(req);
            LOG.debug("create index success for topic = {}, tokenizer = {}, requestId = {}", topicId,
                    tokenizer, createIndexResponse.getRequestId());
        } catch (Exception e) {
            String errMsg = "Create cls topic index failed: " + e.getMessage();
            LOG.error(errMsg, e);
            throw new BusinessException(errMsg);
        }

    }

    /**
     * Describe cls topicName by topic id
     */
    public String describeTopicNameByTopicId(String topicId, String logSetId, String secretId, String secretKey,
            String region) {
        ClsClient clsClient = getClsClient(secretId, secretKey, region);
        Filter[] filters = getDescribeFilters(topicId, logSetId);
        DescribeTopicsRequest req = new DescribeTopicsRequest();
        req.setFilters(filters);
        req.setPreciseSearch(PRECISE_SEARCH);
        try {
            DescribeTopicsResponse describeTopicsResponse = clsClient.DescribeTopics(req);
            if (ArrayUtils.isNotEmpty(describeTopicsResponse.getTopics())) {
                TopicInfo[] topics = describeTopicsResponse.getTopics();
                return topics[0].getTopicName();
            }
            return null;
        } catch (Exception e) {
            String errMsg = "describe cls topic failed: " + e.getMessage();
            LOG.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    public void modifyTopicNameByTopicId(String topicId, String topicName, String secretId, String secretKey,
            String region) {
        ClsClient clsClient = getClsClient(secretId, secretKey, region);
        ModifyTopicRequest req = new ModifyTopicRequest();
        req.setTopicId(topicId);
        req.setTopicName(topicName);
        try {
            ModifyTopicResponse modifyTopicResponse = clsClient.ModifyTopic(req);
            LOG.info("modify cls topic name success for topicId={}, topicName={}", topicId, topicName);
        } catch (Exception e) {
            String errMsg = "modify cls topic name failed: " + e.getMessage();
            LOG.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    public Filter[] getDescribeFilters(String topicId, String logSetId) {
        Filter topicIdFilter = new Filter();
        topicIdFilter.setKey(TOPIC_ID);
        String[] topicIdFilterValues = new String[]{topicId};
        topicIdFilter.setValues(topicIdFilterValues);

        Filter logSetIdFilter = new Filter();
        logSetIdFilter.setKey(LOG_SET_ID);
        String[] logSetFilterValues = new String[]{logSetId};
        logSetIdFilter.setValues(logSetFilterValues);
        return new Filter[]{topicIdFilter, logSetIdFilter};
    }

    /**
     * Get cls topic index full text
     */
    public FullTextInfo getTopicIndexFullText(String secretId, String secretKey, String region, String topicId) {

        ClsClient clsClient = getClsClient(secretId, secretKey, region);
        DescribeIndexRequest req = new DescribeIndexRequest();
        req.setTopicId(topicId);
        try {
            DescribeIndexResponse resp = clsClient.DescribeIndex(req);
            return resp.getRule() == null ? null : resp.getRule().getFullText();
        } catch (Exception e) {
            String errMsg = "describe cls topic index failed: " + e.getMessage();
            LOG.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    public void updateTopicIndex(String tokenizer, String topicId, String secretId, String secretKey, String region) {
        ClsClient clsClient = getClsClient(secretId, secretKey, region);
        RuleInfo ruleInfo = new RuleInfo();
        FullTextInfo fullTextInfo = new FullTextInfo();
        fullTextInfo.setTokenizer(tokenizer);
        ruleInfo.setFullText(fullTextInfo);

        ModifyIndexRequest req = new ModifyIndexRequest();
        req.setTopicId(topicId);
        req.setRule(ruleInfo);
        try {
            ModifyIndexResponse modifyIndexResponse = clsClient.ModifyIndex(req);
            LOG.debug("update index success for topicId = {}, tokenizer = {}, requestId = {}", topicId, tokenizer,
                    modifyIndexResponse.getRequestId());
        } catch (Exception e) {
            String errMsg = "update cls topic index failed: " + e.getMessage();
            LOG.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    public ClsClient getClsClient(String secretId, String secretKey, String region) {
        Credential cred = new Credential(secretId,
                secretKey);
        HttpProfile httpProfile = new HttpProfile();
        httpProfile.setEndpoint(endpoint);
        ClientProfile clientProfile = new ClientProfile();

        clientProfile.setHttpProfile(httpProfile);
        return new ClsClient(cred, region, clientProfile);
    }

    public CreateIndexRequest getCreateIndexRequest(String tokenizer, String topicId) {
        RuleInfo ruleInfo = new RuleInfo();
        FullTextInfo fullTextInfo = new FullTextInfo();
        fullTextInfo.setTokenizer(tokenizer);
        ruleInfo.setFullText(fullTextInfo);

        CreateIndexRequest req = new CreateIndexRequest();
        req.setTopicId(topicId);
        req.setRule(ruleInfo);
        return req;
    }

    public CreateTopicRequest getCreateTopicRequest(String tags, String logSetId, String topicName,
            Integer storageDuration) {
        CreateTopicRequest req = new CreateTopicRequest();
        req.setTags(convertTags(tags.split(InlongConstants.CENTER_LINE)));
        req.setLogsetId(logSetId);
        req.setTopicName(topicName);
        req.setPeriod(storageDuration == null ? null : Long.valueOf(storageDuration));
        return req;
    }

    public Tag[] convertTags(String[] allTags) {
        List<Tag> tagList = new ArrayList<>();
        for (String tag : allTags) {
            String[] keyAndValueOfTag = tag.split(InlongConstants.COLON);
            if (keyAndValueOfTag.length < 2) {
                continue;
            }
            Tag tagInfo = new Tag();
            tagInfo.setKey(keyAndValueOfTag[0]);
            tagInfo.setValue(keyAndValueOfTag[1]);
            tagList.add(tagInfo);
        }
        return tagList.toArray(new Tag[0]);
    }

}
