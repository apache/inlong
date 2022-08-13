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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Log masker used for masking the contents of a log event. The maskers are executed one after the other
 * starting with the last known unmasked position
 */
public interface LogMasker {
    List<Character> KNOWN_DELIMITERS = Collections.unmodifiableList(Arrays.asList('\'', '"', '<', '>'));

    static boolean isDelimiter(char character) {
        return Character.isWhitespace(character) || KNOWN_DELIMITERS.contains(character);
    }

    static int indexOfNextDelimiter(StringBuilder builder, int startPos, int buffLength) {
        while (startPos < buffLength && !isDelimiter(builder.charAt(startPos))) {
            startPos++;
        }
        return startPos;
    }

    default void initialize(String params){
    }

    /**
     * Mask the StringBuilder starting at the start position
     * @param builder The StringBuilder which contains the text that should be masked
     * @param maskChar The character used for masking
     * @param startPos The starting position from which masking should be attempted
     * @param buffLength The total length of the buffer
     * @return Return the starting position for the next masker. If no masking was done, the end position should be
     *      the same as the start position. If masking was done, the end position should be the end of the masked text.
     */
    int maskData(StringBuilder builder, char maskChar, int startPos, int buffLength);
}
