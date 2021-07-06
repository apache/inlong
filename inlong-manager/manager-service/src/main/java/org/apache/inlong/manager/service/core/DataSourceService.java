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

package org.apache.inlong.manager.service.core;

import java.util.List;
import org.apache.inlong.manager.common.pojo.query.ColumnInfoBean;
import org.apache.inlong.manager.common.pojo.query.ConnectionInfo;
import org.apache.inlong.manager.common.pojo.query.DatabaseDetail;

public interface DataSourceService<D, T> {

    /**
     * Test whether the connection can be successfully established.
     *
     * @return true or false
     */
    boolean testConnection(ConnectionInfo connectionInfo);

    void createDb(T queryBean) throws Exception;

    void dropDb(D queryBean) throws Exception;

    void createTable(T queryBean) throws Exception;

    void dropTable(T queryBean) throws Exception;

    void createColumn(T queryBean) throws Exception;

    void updateColumn(T queryBean) throws Exception;

    void dropColumn(T queryBean) throws Exception;

    List<ColumnInfoBean> queryColumns(T queryBean) throws Exception;

    DatabaseDetail queryDbDetail(T queryBean) throws Exception;

}
