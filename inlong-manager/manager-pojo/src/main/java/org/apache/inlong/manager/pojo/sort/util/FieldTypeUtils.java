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

package org.apache.inlong.manager.pojo.sort.util;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.enums.ErrorCodeEnum;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.inlong.manager.pojo.common.FieldTypeMapperInfo;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Util for field type convert.
 */
@Data
@Component
@ConfigurationProperties(prefix = "type-mapper")
public class FieldTypeUtils {

    private static String FIELD_TYPE_CONFIG_YAML = "fieldMapper.yml";
    private static Map<String, Map<String, String>> streamTypeConvert;
    private static Map<String, Map<String, String>> sinkTypeConvert;
    private Map<String, List<FieldTypeMapperInfo>> streamTypeMapper;
    private Map<String, List<FieldTypeMapperInfo>> sinkTypeMapper;

    @Bean
    public static PropertySourcesPlaceholderConfigurer properties() {
        PropertySourcesPlaceholderConfigurer configurer = new PropertySourcesPlaceholderConfigurer();
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource(FIELD_TYPE_CONFIG_YAML));
        configurer.setProperties(Objects.requireNonNull(yaml.getObject()));
        return configurer;
    }

    public Map<String, Map<String, String>> getStreamTypeConvertMap() {
        if (streamTypeConvert == null || streamTypeConvert.isEmpty()) {
            synchronized (FieldTypeUtils.class) {
                if (streamTypeConvert == null || streamTypeConvert.isEmpty()) {
                    streamTypeConvert = new HashMap<>();
                    streamTypeMapper.forEach((s, fieldTypeMapperBeans) -> {
                        streamTypeConvert.put(s, fieldTypeMapperBeans.stream().collect(
                                Collectors.toMap(FieldTypeMapperInfo::getSourceType,
                                        FieldTypeMapperInfo::getTargetType)));
                    });
                }
            }
        }
        return streamTypeConvert;
    }

    public Map<String, Map<String, String>> getSinkTypeConvertMap() {
        if (sinkTypeConvert == null || sinkTypeConvert.isEmpty()) {
            synchronized (FieldTypeUtils.class) {
                if (sinkTypeConvert == null || sinkTypeConvert.isEmpty()) {
                    sinkTypeConvert = new HashMap<>();
                    sinkTypeMapper.forEach((s, fieldTypeMapperBeans) -> {
                        sinkTypeConvert.put(s, fieldTypeMapperBeans.stream().collect(
                                Collectors.toMap(FieldTypeMapperInfo::getSourceType,
                                        FieldTypeMapperInfo::getTargetType)));
                    });
                }
            }
        }
        return sinkTypeConvert;
    }

    public String getStreamField(String sourceType, String fieldType) {
        Map<String, Map<String, String>> typeMap = getStreamTypeConvertMap();
        Map<String, String> fieldTypeMap = typeMap.get(sourceType);
        Preconditions.expectNotEmpty(fieldTypeMap,
                String.format(ErrorCodeEnum.SOURCE_TYPE_NOT_SUPPORT.getMessage(), sourceType));
        String targetFieldType = fieldTypeMap.get(fieldType);
        return StringUtils.isNotBlank(targetFieldType) ? targetFieldType : "string";
    }

    public String getSinkField(String sourceType, String fieldType) {
        Map<String, Map<String, String>> typeMap = getSinkTypeConvertMap();
        Map<String, String> fieldTypeMap = typeMap.get(sourceType);
        Preconditions.expectNotEmpty(fieldTypeMap,
                String.format(ErrorCodeEnum.SINK_TYPE_NOT_SUPPORT.getMessage(), sourceType));
        String targetFieldType = fieldTypeMap.get(fieldType);
        return StringUtils.isNotBlank(targetFieldType) ? targetFieldType : "string";
    }
}
