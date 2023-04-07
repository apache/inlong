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
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Util for field type convert.
 */
@Data
@Component
public class FieldTypeUtils {

    private static String FIELD_TYPE_CONFIG_YAML = "FieldMapper.yml";
    private static String SOUCE_TYPE = "sourceType";
    private static String TARGET_TYPE = "targetType";

    public Map<String, String> getTypeConvertMap(String sinkType, boolean isSink) throws Exception {
        Map<String, String> typeMap = new HashMap<>();
        String yamlName = isSink ? "sink" + FIELD_TYPE_CONFIG_YAML : "stream" + FIELD_TYPE_CONFIG_YAML;
        String path = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("")).getPath()
                + yamlName;
        Yaml yaml = new Yaml();
        Map<String, Map<String, String>> map = yaml.load(new FileInputStream(path));
        List<Map<String, String>> list = (List<Map<String, String>>) map.get(sinkType);
        list.forEach(s -> {
            typeMap.put(s.get(SOUCE_TYPE), s.get(TARGET_TYPE));
        });
        return typeMap;
    }

    public String getField(String sinkType, String fieldType, boolean isSink) throws Exception {
        Map<String, String> typeMap = getTypeConvertMap(sinkType, isSink);
        String targetFieldType = typeMap.get(fieldType);
        return StringUtils.isNotBlank(targetFieldType) ? targetFieldType : "string";
    }
}
