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

package org.apache.inlong.manager.common.pojo.query.hive;

import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.inlong.manager.common.pojo.query.TableQueryBean;

/**
 * Bean of query hive table
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class HiveTableQueryBean extends TableQueryBean {

    private String serdeName;
    private String serdeProperties;
    private String jdbcUrl;
    private String username;
    private String password;
    private String fieldTerSymbol;
    private String collectionTerSymbol;
    private String mapTerSymbol;
    private String linesTerSymbol;

    private String fileFormat;
    private String inputFormatClass;
    private String outputFormatClass;

    private String location;
    private String loadPath;
    private boolean firstColumn;
    private Map<String, String> tblProperties;

    // Hive fields
    private List<HiveColumnQueryBean> columns;

}
