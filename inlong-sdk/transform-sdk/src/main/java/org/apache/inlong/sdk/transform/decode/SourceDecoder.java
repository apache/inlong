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

import org.apache.inlong.sdk.transform.pojo.FieldInfo;
import org.apache.inlong.sdk.transform.process.Context;

import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.util.List;

/**
 * SourceDecoder
 */
@Getter
public abstract class SourceDecoder<Input> {

    protected final List<FieldInfo> fields;

    public SourceDecoder() {
        this(ImmutableList.of());
    }

    public SourceDecoder(List<FieldInfo> fields) {
        this.fields = fields;
    }

    public abstract SourceData decode(byte[] srcBytes, Context context);

    public abstract SourceData decode(Input input, Context context);

}
