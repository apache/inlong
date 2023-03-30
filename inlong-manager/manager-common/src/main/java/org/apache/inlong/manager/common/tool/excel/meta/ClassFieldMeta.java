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

package org.apache.inlong.manager.common.tool.excel.meta;

import org.apache.inlong.manager.common.tool.excel.validator.ExcelCellValidator;
import org.apache.inlong.manager.common.tool.excel.ExcelCellDataTransfer;

import lombok.Data;
import java.io.Serializable;
import java.lang.reflect.Field;

@Data
public class ClassFieldMeta implements Serializable {

    /**
    
     * The name of the field
     */
    private String name;

    /**
     * The name of the field in the excel sheet
     */
    private String excelName;

    /**
     * The type of the field
     */
    private Class<?> fieldType;

    /**
     * The validator for the cell
     */
    private ExcelCellValidator<?> cellValidator;

    /**
     * The data transfer for the cell
     */
    private ExcelCellDataTransfer cellDataTransfer;

    /**
     * The field object
     */
    private Field filed;

    public ClassFieldMeta() {
    }
}
