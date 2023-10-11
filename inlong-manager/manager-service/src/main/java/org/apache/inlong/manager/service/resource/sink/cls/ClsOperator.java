package org.apache.inlong.manager.service.resource.sink.cls;

import com.tencentcloudapi.cls.v20201016.ClsClient;
import com.tencentcloudapi.cls.v20201016.models.CreateIndexRequest;
import com.tencentcloudapi.cls.v20201016.models.CreateTopicRequest;
import com.tencentcloudapi.cls.v20201016.models.CreateTopicResponse;
import com.tencentcloudapi.cls.v20201016.models.DescribeIndexRequest;
import com.tencentcloudapi.cls.v20201016.models.DescribeIndexResponse;
import com.tencentcloudapi.cls.v20201016.models.DescribeTopicsRequest;
import com.tencentcloudapi.cls.v20201016.models.DescribeTopicsResponse;
import com.tencentcloudapi.cls.v20201016.models.Filter;
import com.tencentcloudapi.cls.v20201016.models.FullTextInfo;
import com.tencentcloudapi.cls.v20201016.models.ModifyIndexRequest;
import com.tencentcloudapi.cls.v20201016.models.RuleInfo;
import com.tencentcloudapi.cls.v20201016.models.Tag;
import com.tencentcloudapi.cls.v20201016.models.TopicInfo;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.exceptions.BusinessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ClsOperator {

    private static final Logger LOG = LoggerFactory.getLogger(ClsOperator.class);
    private static final String TOPIC_NAME = "topicName";
    private static final String LOG_SET_ID = "logsetId";
    private static final long PRECISE_SEARCH = 1L;

    public String createTopicReturnTopicId(String topicName, String logSetId, String tag, String secretId,
            String secretKey, String endPoint, String region)
            throws TencentCloudSDKException {
        ClsClient client = getClsClient(secretId, secretKey, endPoint, region);
        CreateTopicRequest req = getCreateTopicRequest(tag, logSetId, topicName);
        CreateTopicResponse resp = client.CreateTopic(req);
        LOG.info("create cls topic success for topicName = {}, topicId = {}", topicName,
                resp.getTopicId());
        return resp.getTopicId();
    }

    /**
     * Create topic index by tokenizer
     */
    public void createTopicIndex(String tokenizer, String topicId, String secretId, String secretKey, String
            endPoint, String region) throws BusinessException {

        LOG.debug("create topic index start for topicId = {}, tokenizer = {}",topicId,tokenizer);
        if (StringUtils.isBlank(tokenizer)) {
            LOG.warn("tokenizer is blank for topic = {}", topicId);
            return;
        }
        FullTextInfo topicIndexFullText = getTopicIndexFullText(secretId, secretKey, endPoint, region, topicId);
        if (ObjectUtils.anyNotNull(topicIndexFullText)) {
            // if topic index exist,update
            LOG.debug("cls topic is exist and update for topicId = {},tokenizer = {}",topicId,tokenizer);
            updateTopicIndex(tokenizer, topicId, secretId, secretKey, endPoint, region);
            return;
        }
        ClsClient clsClient = getClsClient(secretId, secretKey, endPoint, region);
        CreateIndexRequest req = getCreateIndexRequest(tokenizer, topicId);
        try {
            clsClient.CreateIndex(req);
        } catch (TencentCloudSDKException e) {
            String errMsg = "Create cls topic index failed: " + e.getMessage();
            LOG.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
        LOG.debug("create index success for topic = {}, tokenizer = {}", topicId,
                tokenizer);
    }

    /**
     * Describe cls topicId by topic name
     */
    public String describeTopicIDByTopicName(String topicName, String logSetId, String tag, String secretId,
            String secretKey, String endPoint, String region) {
        ClsClient clsClient = getClsClient(secretId, secretKey, endPoint, region);
        Filter[] filters = getDescribeFilters(topicName, logSetId);
        DescribeTopicsRequest req = new DescribeTopicsRequest();
        req.setFilters(filters);
        req.setPreciseSearch(PRECISE_SEARCH);
        try {
            DescribeTopicsResponse describeTopicsResponse = clsClient.DescribeTopics(req);
            if (ArrayUtils.isNotEmpty(describeTopicsResponse.getTopics())) {
                TopicInfo[] topics = describeTopicsResponse.getTopics();
                return topics[0].getTopicId();
            }
            return null;
        } catch (TencentCloudSDKException e) {
            String errMsg = "describe cls topic failed: " + e.getMessage();
            LOG.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    public Filter[] getDescribeFilters(String topicName, String logSetId) {
        Filter topicNameFilter = new Filter();
        topicNameFilter.setKey(TOPIC_NAME);
        String[] topicNameFilterValues = new String[]{topicName};
        topicNameFilter.setValues(topicNameFilterValues);

        Filter logSetIdFilter = new Filter();
        logSetIdFilter.setKey(LOG_SET_ID);
        String[] logSetFilterValues = new String[]{logSetId};
        logSetIdFilter.setValues(logSetFilterValues);
        return new Filter[]{topicNameFilter, logSetIdFilter};
    }

    /**
     * Get cls topic index full text
     */
    public FullTextInfo getTopicIndexFullText(String secretId, String secretKey, String
            endPoint, String region, String topicId) {

        ClsClient clsClient = getClsClient(secretId, secretKey, endPoint, region);
        DescribeIndexRequest req = new DescribeIndexRequest();
        req.setTopicId(topicId);
        try {
            DescribeIndexResponse resp = clsClient.DescribeIndex(req);
            return resp.getRule() == null ? null : resp.getRule().getFullText();
        } catch (TencentCloudSDKException e) {
            String errMsg = "describe cls topic index failed: " + e.getMessage();
            LOG.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
    }

    public void updateTopicIndex(String tokenizer, String topicId,
            String secretId, String secretKey, String endPoint, String region) {
        ClsClient clsClient = getClsClient(secretId, secretKey, endPoint, region);
        RuleInfo ruleInfo = new RuleInfo();
        FullTextInfo fullTextInfo = new FullTextInfo();
        fullTextInfo.setTokenizer(tokenizer);
        ruleInfo.setFullText(fullTextInfo);

        ModifyIndexRequest req = new ModifyIndexRequest();
        req.setTopicId(topicId);
        req.setRule(ruleInfo);
        try {
            clsClient.ModifyIndex(req);
        } catch (TencentCloudSDKException e) {
            String errMsg = "update cls topic index failed: " + e.getMessage();
            LOG.error(errMsg, e);
            throw new BusinessException(errMsg);
        }
        LOG.debug("update index success for topicId = {}, tokenizer = {}", topicId, tokenizer);
    }

    public ClsClient getClsClient(String secretId, String secretKey, String endPoint, String region) {
        Credential cred = new Credential(secretId,
                secretKey);
        HttpProfile httpProfile = new HttpProfile();
        httpProfile.setEndpoint(endPoint);
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

    public CreateTopicRequest getCreateTopicRequest(String tags, String logSetId, String topicName) {
        CreateTopicRequest req = new CreateTopicRequest();
        req.setTags(convertTags(tags.split(InlongConstants.CENTER_LINE)));
        req.setLogsetId(logSetId);
        req.setTopicName(topicName);
        return req;
    }

    public Tag[] convertTags(String[] allTags) {
        List<Tag> tagList = new ArrayList<>();
        int noTag = 2;
        for (String tag : allTags) {
            String[] keyAndValueOfTag = tag.split(InlongConstants.COLON);
            if (keyAndValueOfTag.length < noTag) {
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
