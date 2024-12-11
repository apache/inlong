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

package org.apache.inlong.manager.schedule.airflow.config;

import org.apache.inlong.manager.client.api.ClientConfiguration;
import org.apache.inlong.manager.schedule.airflow.AirflowServerClient;
import org.apache.inlong.manager.schedule.airflow.interceptor.AirflowAuthInterceptor;
import org.apache.inlong.manager.schedule.airflow.interceptor.LoggingInterceptor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import okhttp3.OkHttpClient;
import org.eclipse.jetty.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

import java.net.URL;

@Data
@Configuration
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class AirflowConfig extends ClientConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(AirflowConfig.class);
    @Value("${schedule.engine.inlong.manager.url:http://127.0.0.1:8083}")
    private String inlongManagerUrl;

    private String inlongManagerHost;
    private int inlongManagerPort;

    @Value("${default.admin.user:admin}")
    private String inlongUsername;

    @Value("${default.admin.password:inlong}")
    private String inlongPassword;

    @Value("${schedule.engine.airflow.connection.id:inlong_connection}")
    private String connectionId;

    @Value("${schedule.engine.airflow.cleaner.id:dag_cleaner}")
    private String dagCleanerId;

    @Value("${schedule.engine.airflow.creator.id:dag_creator}")
    private String dagCreatorId;

    @Value("${schedule.engine.airflow.username:airflow}")
    private String airflowUsername;

    @Value("${schedule.engine.airflow.password:airflow}")
    private String airflowPassword;

    @Value("${schedule.engine.airflow.baseUrl:http://localhost:8080/}")
    private String baseUrl;

    @PostConstruct
    public void init() {
        try {
            if (StringUtil.isNotBlank(inlongManagerUrl)) {
                URL url = new URL(inlongManagerUrl);
                this.inlongManagerHost = url.getHost();
                this.inlongManagerPort = url.getPort();
                if (this.inlongManagerPort == -1) {
                    this.inlongManagerPort = 8083;
                }
            }
            LOGGER.info("Init AirflowConfig success for manager url ={}", this.inlongManagerUrl);
        } catch (Exception e) {
            LOGGER.error("Init AirflowConfig failed for manager url={}: ", this.inlongManagerUrl, e);
        }
    }

    @Bean
    public OkHttpClient okHttpClient() {
        return new OkHttpClient.Builder()
                .addInterceptor(new AirflowAuthInterceptor(this.getAirflowUsername(), this.getAirflowPassword()))
                .addInterceptor(new LoggingInterceptor())
                .connectTimeout(this.getConnectTimeout(), this.getTimeUnit())
                .readTimeout(this.getReadTimeout(), this.getTimeUnit())
                .writeTimeout(this.getWriteTimeout(), this.getTimeUnit())
                .retryOnConnectionFailure(true)
                .build();
    }

    @Bean
    public AirflowServerClient airflowServerClient(OkHttpClient okHttpClient, AirflowConfig airflowConfig) {
        return new AirflowServerClient(okHttpClient, airflowConfig);
    }
}
