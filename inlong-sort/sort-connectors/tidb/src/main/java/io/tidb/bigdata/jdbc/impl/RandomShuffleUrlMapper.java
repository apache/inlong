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

package io.tidb.bigdata.jdbc.impl;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

public class RandomShuffleUrlMapper implements Function<String[], String[]> {

    /**
     * apply
     *
     * @param input urls
     */
    @Override
    public String[] apply(final String[] input) {
        Random random = ThreadLocalRandom.current();
        int size = input.length;
        String[] shuffled = Arrays.copyOf(input, size);
        for (int i = size; i > 1; i--) {
            int j = random.nextInt(i);
            String tmp = shuffled[i - 1];
            shuffled[i - 1] = shuffled[j];
            shuffled[j] = tmp;
        }
        return shuffled;
    }
}
