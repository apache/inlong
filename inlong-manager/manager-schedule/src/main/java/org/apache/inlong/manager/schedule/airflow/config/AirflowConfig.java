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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class AirflowConfig extends ClientConfiguration {

    @Value("${schedule.engine.inlong.manager.host:127.0.0.1}")
    private String host;

    @Value("${server.port:8083}")
    private int port;

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
