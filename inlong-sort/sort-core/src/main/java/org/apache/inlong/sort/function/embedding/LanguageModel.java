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

package org.apache.inlong.sort.function.embedding;

import com.google.common.base.Strings;

/**
 * Supported language model for embedding.
 */
public enum LanguageModel {

    /**
     * Language model for BBAI zh, chinese
     * */
    BBAI_ZH("BAAI/bge-large-zh-v1.5"),
    /**
     * Language model for BBAI en, english
     * */
    BBAI_EN("BAAI/bge-large-en"),
    /**
     * Language model for intfloat multi-language
     * */
    INTFLOAT_MULTI("intfloat/multilingual-e5-large");
    String model;

    LanguageModel(String s) {
        this.model = s;
    }

    public String getModel() {
        return this.model;
    }

    public static boolean isLanguageModelSupported(String s) {
        if (Strings.isNullOrEmpty(s)) {
            return false;
        }
        for (LanguageModel lm : LanguageModel.values()) {
            if (s.equalsIgnoreCase(lm.getModel())) {
                return true;
            }
        }
        return false;
    }

    public static String getAllSupportedLanguageModels() {
        if (LanguageModel.values().length == 0) {
            return null;
        }
        StringBuilder supportedLMBuilder = new StringBuilder();
        for (LanguageModel lm : LanguageModel.values()) {
            supportedLMBuilder.append(lm.getModel()).append(",");
        }
        return supportedLMBuilder.substring(0, supportedLMBuilder.length() - 1);
    }
}
