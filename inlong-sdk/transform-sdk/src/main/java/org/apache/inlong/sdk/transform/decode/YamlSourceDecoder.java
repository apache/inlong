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

package org.apache.inlong.sdk.transform.decode;

import org.apache.inlong.sdk.transform.pojo.YamlSourceInfo;
import org.apache.inlong.sdk.transform.process.Context;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class YamlSourceDecoder implements SourceDecoder<String> {

    protected YamlSourceInfo sourceInfo;
    private Charset srcCharset = Charset.defaultCharset();
    private String rowsNodePath;
    private List<String> childNodes;

    public YamlSourceDecoder(YamlSourceInfo sourceInfo) {
        this.sourceInfo = sourceInfo;
        if (!StringUtils.isBlank(sourceInfo.getCharset())) {
            this.srcCharset = Charset.forName(sourceInfo.getCharset());
        }
        this.rowsNodePath = sourceInfo.getRowsNodePath();
        if (!StringUtils.isBlank(rowsNodePath)) {
            this.childNodes = new ArrayList<>();
            String[] nodeStrings = this.rowsNodePath.split("\\.");
            childNodes.addAll(Arrays.asList(nodeStrings));
        }
    }
    @Override
    public SourceData decode(byte[] srcBytes, Context context) {
        String srcString = new String(srcBytes, srcCharset);
        return this.decode(srcString, context);
    }

    @Override
    public SourceData decode(String srcString, Context context) {
        try {
            Yaml yaml = new Yaml();
            Map<String, Object> yamlData = yaml.load(srcString);
            Map<String, YamlNode> rootMap = new HashMap<>();
            List<YamlNode> childList = new ArrayList<>();

            for (Map.Entry<String, Object> entry : yamlData.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (value instanceof Map) {
                    Map<String, YamlNode> childNodes = parser((Map<String, Object>) value);
                    rootMap.put(key, new YamlNode(key, childNodes));
                } else if (value instanceof List) {
                    for (Object item : (List<?>) value) {
                        if (item instanceof Map) {
                            Map<String, YamlNode> childNodes = parser((Map<String, Object>) item);
                            childList.add(new YamlNode(key, childNodes));
                        } else {
                            childList.add(new YamlNode(key, item));
                        }
                    }
                    rootMap.put(key, new YamlNode(key, value));
                } else {
                    rootMap.put(key, new YamlNode(key, value));
                }
            }
            YamlNode root = new YamlNode(YamlSourceData.ROOT_KEY, rootMap.isEmpty() ? null : rootMap);
            YamlNode childRoot = new YamlNode(YamlSourceData.CHILD_KEY, rowsNodePath.isEmpty() ? null : childList);
            return new YamlSourceData(root, childRoot);
        } catch (Exception e) {
            log.error("Data parsing failed", e);
            return null;
        }
    }

    private static Map<String, YamlNode> parser(Map<String, Object> yamlData) {
        Map<String, YamlNode> yamlNodes = new HashMap<>();
        for (Map.Entry<String, Object> entry : yamlData.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                yamlNodes.put(key, new YamlNode(key, parser((Map<String, Object>) value)));
            } else if (value instanceof List) {
                List<YamlNode> list = new ArrayList<>();
                for (Object item : (List<?>) value) {
                    if (item instanceof Map) {
                        list.add(new YamlNode(key, parser((Map<String, Object>) item)));
                    } else {
                        list.add(new YamlNode(key, item));
                    }
                }
                yamlNodes.put(key, new YamlNode(key, list));
            } else {
                yamlNodes.put(key, new YamlNode(key, value));
            }
        }
        return yamlNodes;
    }
}
