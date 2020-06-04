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

package org.apache.tubemq.corebase.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.apache.tubemq.corebase.exception.CompressionException;


public enum CompressionType {

    NONE("none") {
        @Override
        public byte[] uncompress(byte[] buffer) {
            return buffer;
        }

        @Override
        public byte[] compress(byte[] buffer) {
            return buffer;
        }
    },

    GZIP("gzip") {
        @Override
        public byte[] uncompress(byte[] src) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteArrayInputStream in = new ByteArrayInputStream(src);
            try {
                GZIPInputStream gzip = new GZIPInputStream(in);
                byte[] tmp = new byte[1024];
                int offset;
                while ((offset = gzip.read(tmp)) >= 0) {
                    out.write(tmp, 0, offset);
                }
                return out.toByteArray();
            } catch (Exception e) {
                throw new CompressionException(e);
            } finally {
                try {
                    in.close();
                } catch (Exception ignore){}
                try {
                    out.close();
                } catch (Exception ignore){}
            }
        }

        @Override
        public byte[] compress(byte[] src) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                GZIPOutputStream gzip = new GZIPOutputStream(baos);
                gzip.write(src);
                gzip.close();
                return baos.toByteArray();
            } catch (Exception e) {
                throw new CompressionException(e);
            } finally {
                try {
                    baos.close();
                } catch (Exception ignore){}
            }
        }
    };

    String name;

    CompressionType(String name) {
        this.name = name;
    }

    public abstract byte[] uncompress(byte[] src);

    public abstract byte[] compress(byte[] src);

}
