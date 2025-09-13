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

package org.apache.inlong.audit.tool.util;

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

import org.apache.inlong.manager.common.util.Preconditions;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

/**
 * HTTP utils
 */
@Slf4j
public class HttpUtil {

    private static final Gson GSON = new GsonBuilder().create(); // thread safe

    /**
     * Send an HTTP request
     */
    public static <T> T request(RestTemplate restTemplate, String url, HttpMethod httpMethod, Object requestBody,
            MultiValueMap<String, String> header, ParameterizedTypeReference<T> typeReference) {
        if (log.isDebugEnabled()) {
            log.debug("begin request to {} by request body {}", url, GSON.toJson(requestBody));
        }
        HttpEntity<Object> requestEntity = new HttpEntity<>(requestBody, header);
        ResponseEntity<T> response = restTemplate.exchange(url, httpMethod, requestEntity, typeReference);

        log.debug("success request to {}, status code {}", url, response.getStatusCode());
        Preconditions.expectTrue(response.getStatusCode().is2xxSuccessful(), "Request failed: " + response.getBody());
        return response.getBody();
    }

}
