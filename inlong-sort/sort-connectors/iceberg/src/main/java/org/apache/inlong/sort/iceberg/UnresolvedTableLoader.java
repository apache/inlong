/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements. See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.inlong.sort.iceberg;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public interface UnresolvedTableLoader extends Closeable, Serializable {
    void open();

    Table loadTable(String table);

    public static class UnresolvedCatalogTableLoader implements UnresolvedTableLoader {
        private static final long serialVersionUID = 1L;
        private final CatalogLoader catalogLoader;
        private transient Catalog catalog;

        private UnresolvedCatalogTableLoader(CatalogLoader catalogLoader) {
            this.catalogLoader = catalogLoader;
        }

        @Override
        public void open() {
            this.catalog = this.catalogLoader.loadCatalog();
        }

        @Override
        public Table loadTable(String table) {
            return this.catalog.loadTable(TableIdentifier.parse(table));
        }

        @Override
        public void close() throws IOException {
            if (this.catalog instanceof Closeable) {
                ((Closeable)this.catalog).close();
            }

        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("catalogLoader", this.catalogLoader)
                    .toString();
        }
    }
}
