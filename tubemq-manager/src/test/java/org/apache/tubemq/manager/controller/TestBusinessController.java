/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tubemq.manager.controller;

import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.apache.tubemq.manager.entry.BusinessEntry;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(webEnvironment= WebEnvironment.RANDOM_PORT)
@Slf4j
public class TestBusinessController {

    @Autowired
    private TestRestTemplate client;

    @LocalServerPort
    private int randomServerPort;

    private MockMvc mvc;

    @Before
    public void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(new BusinessController()).build();
    }

    @Test
    public void test404Controller() throws Exception {
        RequestBuilder request;
        // get request, path not exists
        request = get("/business");
        mvc.perform(request)
                .andExpect(status().isNotFound());
    }

    @Test
    public void testAddBusiness() throws Exception {
        final String baseUrl = "http://localhost:" + randomServerPort + "/business/add";
        URI uri = new URI(baseUrl);
        String demoName = "test";
        BusinessEntry entry = new BusinessEntry(demoName, demoName, demoName,
                demoName, demoName, demoName);

        HttpHeaders headers = new HttpHeaders();
        HttpEntity<BusinessEntry> request = new HttpEntity<>(entry, headers);

        ResponseEntity<?> responseEntity =
                client.postForEntity(uri, request, ResponseEntity.class);
        assertThat(responseEntity.getStatusCode().is2xxSuccessful()).isEqualTo(true);
    }
}
