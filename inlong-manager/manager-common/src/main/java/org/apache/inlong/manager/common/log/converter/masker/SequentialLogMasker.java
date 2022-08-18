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

package org.apache.inlong.manager.common.log.converter.masker;

/**
 * Log Masker used for masking a text. All maskers of type SequentialLogmasker will be executed one after the other,
 * with the entire text being sent
 */
public interface SequentialLogMasker {
    /**
     * Reset the masker configuration using the provided arguments
     * @param args The arguments
     */
    default void initialize(String args) {
    }

    /**
     * Mask the content of the supplied String Buffer using the provided mask character
     * @param unmasked The StringBuffer that should be masked
     * @param maskChar The character that will be used for masking purposes
     * @return If anything was masked or not
     */
    boolean mask(StringBuilder unmasked, char maskChar);
}
