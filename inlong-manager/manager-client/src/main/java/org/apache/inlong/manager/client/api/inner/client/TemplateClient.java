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

package org.apache.inlong.manager.client.api.inner.client;

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.client.api.service.TemplateApi;
import org.apache.inlong.manager.client.api.util.ClientUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.common.Response;
import org.apache.inlong.manager.pojo.stream.TemplateInfo;
import org.apache.inlong.manager.pojo.stream.TemplatePageRequest;
import org.apache.inlong.manager.pojo.stream.TemplateRequest;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Client for {@link TemplateApi}.
 */
public class TemplateClient {

    private final TemplateApi templateApi;

    public TemplateClient(ClientConfiguration configuration) {
        templateApi = ClientUtils.createRetrofit(configuration).create(TemplateApi.class);
    }

    /**
     * Create an template.
     */
    public Integer save(TemplateRequest request) {
        Response<Integer> response = ClientUtils.executeHttpCall(templateApi.save(request));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Query whether the template name exists
     *
     * @param templateName template name
     * @return true: exists, false: does not exist
     */
    public Boolean exists(String templateName) {
        Preconditions.expectNotBlank(templateName, ErrorCodeEnum.TEMPLATE_INFO_INCORRECT);
        Response<Boolean> response = ClientUtils.executeHttpCall(templateApi.exists(templateName));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Template info that needs to be modified
     *
     * @param request template info that needs to be modified
     * @return whether succeed
     */
    public Pair<Boolean, String> update(TemplateRequest request) {
        Response<Boolean> resp = ClientUtils.executeHttpCall(templateApi.update(request));

        if (resp.getData() != null) {
            return Pair.of(resp.getData(), resp.getErrMsg());
        } else {
            return Pair.of(false, resp.getErrMsg());
        }
    }

    /**
     * Get template by the given template name.
     */
    public TemplateInfo get(String templateName) {
        Response<TemplateInfo> response = ClientUtils.executeHttpCall(templateApi.get(templateName));

        if (response.isSuccess()) {
            return response.getData();
        }
        if (response.getErrMsg().contains("not exist")) {
            return null;
        }
        throw new RuntimeException(response.getErrMsg());
    }

    /**
     * Paging query template info list
     *
     * @param request query request
     * @return template list
     */
    public PageResult<TemplateInfo> listByCondition(TemplatePageRequest request) {
        Response<PageResult<TemplateInfo>> response = ClientUtils.executeHttpCall(
                templateApi.listByCondition(request));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

    /**
     * Delete the specified template
     *
     * @param templateName template name
     * @return whether succeed
     */
    public boolean delete(String templateName) {
        Preconditions.expectNotBlank(templateName, ErrorCodeEnum.TEMPLATE_INFO_INCORRECT);
        Response<Boolean> response = ClientUtils.executeHttpCall(templateApi.delete(templateName));
        ClientUtils.assertRespSuccess(response);
        return response.getData();
    }

}
