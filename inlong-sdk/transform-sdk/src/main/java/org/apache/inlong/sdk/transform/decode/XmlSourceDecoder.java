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

import org.apache.inlong.sdk.transform.pojo.XmlSourceInfo;
import org.apache.inlong.sdk.transform.process.Context;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * XmlSourceDecoder
 */
@Slf4j
public class XmlSourceDecoder extends SourceDecoder<String> {

    protected XmlSourceInfo sourceInfo;
    private Charset srcCharset = Charset.defaultCharset();
    private String rowsNodePath;
    private List<String> childNodes;

    public XmlSourceDecoder(XmlSourceInfo sourceInfo) {
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
            Document doc = DocumentHelper.parseText(srcString);
            Element root = doc.getRootElement();
            XmlNode rootObj = parser(root).get(root.getName());
            Object cur = rootObj.getValue();
            XmlNode child = null;
            if (childNodes != null) {
                for (String node : childNodes) {
                    if (cur instanceof Map) {
                        child = ((Map<String, XmlNode>) cur).get(node);
                    } else if (cur instanceof List) {
                        int start = node.indexOf('(') + 1, end = node.indexOf(')');
                        int idx = Integer.parseInt(node.substring(start, end));
                        child = ((List<XmlNode>) cur).get(idx);
                    }
                    cur = child.getValue();
                }
            }
            return new XmlSourceData(rootObj, child);
        } catch (Exception e) {
            log.error("Data parsing failed", e);
            return null;
        }
    }

    public static Map<String, XmlNode> parser(Element root) {
        Map<String, XmlNode> xmlData = new HashMap<>();
        if (root.isTextOnly()) {
            xmlData.put(root.getName(), new XmlNode(root.getName(), root.getText()));
        } else {
            ArrayList<Map<String, XmlNode>> childNodes = new ArrayList<>();
            for (Object elementObj : root.elements()) {
                Element element = (Element) elementObj;
                childNodes.add(parser(element));
            }
            Map<String, XmlNode> mergeMap = new HashMap<>();
            for (Map<String, XmlNode> childNode : childNodes) {
                for (String key : childNode.keySet()) {
                    XmlNode nowNode = mergeMap.get(key);
                    XmlNode tarNode = childNode.get(key);
                    if (nowNode == null) {
                        mergeMap.put(key, tarNode);
                    } else {
                        if (nowNode.getValue() instanceof List) {
                            ((List<XmlNode>) nowNode.getValue()).add(tarNode);
                        } else {
                            ArrayList<XmlNode> list = new ArrayList<>();
                            list.add(nowNode);
                            list.add(tarNode);
                            mergeMap.put(key, new XmlNode(key, list));
                        }
                    }
                }
            }
            if (mergeMap.size() == 1) {
                XmlNode childValue = new ArrayList<>(mergeMap.values()).get(0);
                xmlData.put(root.getName(), new XmlNode(root.getName(), childValue));
            } else {
                xmlData.put(root.getName(), new XmlNode(root.getName(), mergeMap));
            }
        }
        return xmlData;
    }
}
