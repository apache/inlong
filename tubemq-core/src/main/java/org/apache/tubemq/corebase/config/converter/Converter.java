/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tubemq.corebase.config.converter;

import java.io.Serializable;
import org.apache.tubemq.corebase.config.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IN original data type
 * OUT target value type
 */
public abstract class Converter<I, O> implements Serializable {

    protected static final Logger LOG = LoggerFactory.getLogger(Converter.class);

    /**
     * convert original value to target value.
     * @param in original value
     * @return the target value in specified type.
     */
    public O doConvert(I in) {
        if (in == null) {
            return null;
        }
        O out;
        try {
            out = convert(in);
        } catch (Exception e) {
            throw new ConfigurationException("An error of value convert occurred in " + this.getClass().getName(), e);
        }
        return out;
    }

    /**
     * convert original value to target value.
     * @param in original value
     * @return the target value in specified type.
     */
    protected abstract O convert(I in);
}
