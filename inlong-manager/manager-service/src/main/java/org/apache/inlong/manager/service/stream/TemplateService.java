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

package org.apache.inlong.manager.service.stream;

import org.apache.inlong.manager.pojo.common.PageResult;
import org.apache.inlong.manager.pojo.stream.TemplateInfo;
import org.apache.inlong.manager.pojo.stream.TemplatePageRequest;
import org.apache.inlong.manager.pojo.stream.TemplateRequest;

/**
 * Template service layer interface
 */
public interface TemplateService {

    /**
     * Save inlong template information.
     *
     * @param request Inlong template information.
     * @param operator The name of operator.
     * @return Id after successful save.
     */
    Integer save(TemplateRequest request, String operator);

    /**
     * Query whether the inlong template name exists
     *
     * @param templateName template name
     * @return true: exists, false: does not exist
     */
    Boolean exist(String templateName);

    /**
     * Query the details of the specified inlong template
     *
     * @param templateName Inlong group id
     * @return inlong template details
     */
    TemplateInfo get(String templateName, String operator);

    /**
     * Paging query inlong template info list
     *
     * @param request query request
     * @return inlong template list
     */
    PageResult<TemplateInfo> list(TemplatePageRequest request);

    Boolean update(TemplateRequest request, String operator);

    /**
     * Delete the specified inlong template.
     *
     * @param templateName template name
     * @param operator operator
     * @return whether succeed
     */
    Boolean delete(String templateName, String operator);

    /**
     * Delete the specified inlong template.
     *
     * @param templateId template id
     * @param operator operator
     * @return whether succeed
     */
    Boolean delete(Integer templateId, String operator);

}
