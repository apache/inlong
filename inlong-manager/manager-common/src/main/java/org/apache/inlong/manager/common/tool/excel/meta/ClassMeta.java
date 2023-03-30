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

import org.apache.inlong.manager.common.tool.excel.annotation.ExcelEntity;
import org.apache.inlong.manager.common.tool.excel.annotation.ExcelField;
import org.apache.inlong.manager.common.tool.excel.template.ExcelImportTemplate;
import org.apache.inlong.manager.common.tool.excel.validator.ExcelBatchValidator;
import org.apache.inlong.manager.common.tool.excel.validator.ExcelCellValidator;
import org.apache.inlong.manager.common.tool.excel.validator.ExcelRowValidator;
import org.apache.inlong.manager.common.tool.excel.ExcelCellDataTransfer;

import lombok.Data;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The ClassMeta class represents the metadata of a class that is used to import data from an Excel sheet.
 */
@Data
public class ClassMeta<T extends ExcelImportTemplate> {

    /**
     * The name of the class.
     */
    private String name;

    /**
     * The template class.
     */
    private T tClass;

    /**
     * The validator for a single row.
     */
    private ExcelRowValidator<T> rowValidator;

    /**
     * The validator for a batch of rows.
     */
    private ExcelBatchValidator<T> batchValidator;

    /**
     * The metadata of the fields in the class.
     */
    private List<ClassFieldMeta> classFieldMetas;

    /**
     * The mapping of field names to their metadata.
     */
    private Map<String, ClassFieldMeta> fieldNameMetaMap;

    /**
     * The mapping of Excel names to their metadata.
     */
    private Map<String, ClassFieldMeta> excelNameMetaMap;

    /**
     * Whether the fields have been sorted.
     */
    private boolean isSorted = false;

    /**
     * The mapping of positions to their metadata.
     */
    private Map<Integer, ClassFieldMeta> positionFieldMetaMap;

    /**
     * The method to set whether the Excel data is valid.
     */
    private Method excelDataValidMethod;

    /**
     * The method to set the validation information of the Excel data.
     */
    private Method excelDataValidateInfoMethod;

    private ClassMeta() {
    }

    /**
     * Create a ClassMeta object from a given template class.
     * @param templateClass the template class
     * @param <T> the type of the Excel import template
     * @return the ClassMeta object
     */
    public static <T extends ExcelImportTemplate> ClassMeta<T> of(Class<T> templateClass) {
        ClassMeta<T> meta = new ClassMeta();
        ExcelEntity excelEntity = templateClass.getAnnotation(ExcelEntity.class);
        if (excelEntity != null) {
            meta.name = excelEntity.name();
            Class<? extends ExcelRowValidator> oneRowValidatorClass = excelEntity.oneRowValidator();
            Class<? extends ExcelBatchValidator> batchValidatorClass = excelEntity.batchValidator();

            try {
                if (oneRowValidatorClass != ExcelRowValidator.class) {
                    meta.rowValidator = (ExcelRowValidator) oneRowValidatorClass.newInstance();
                }

                if (batchValidatorClass != ExcelBatchValidator.class) {
                    meta.batchValidator = (ExcelBatchValidator) batchValidatorClass.newInstance();
                }
            } catch (IllegalAccessException | InstantiationException var18) {
                var18.printStackTrace();
            }
        }

        Field[] fields = templateClass.getDeclaredFields();
        int fieldsLength = fields.length;

        for (int index = 0; index < fieldsLength; ++index) {
            Field field = fields[index];
            field.setAccessible(true);
            ExcelField excelField = (ExcelField) field.getAnnotation(ExcelField.class);
            if (excelField != null) {
                String name = field.getName();
                Class<?> type = field.getType();
                String excelName = excelField.name();
                Class<? extends ExcelCellValidator> validatorClass = excelField.validator();
                ExcelCellDataTransfer excelCellDataTransfer = excelField.x2oTransfer();
                ExcelCellValidator excelCellValidator = null;
                if (validatorClass != ExcelCellValidator.class) {
                    try {
                        excelCellValidator = (ExcelCellValidator) validatorClass.newInstance();
                    } catch (IllegalAccessException | InstantiationException var17) {
                        var17.printStackTrace();
                    }
                }

                meta.addField(name, excelName, field, type, excelCellValidator, excelCellDataTransfer);
            }
        }

        try {
            Method excelDataValid = templateClass.getMethod("setExcelDataValid", Boolean.TYPE);
            Method excelDataValidateInfo = templateClass.getMethod("setExcelDataValidate", String.class);
            meta.setExcelDataValidMethod(excelDataValid);
            meta.setExcelDataValidateInfoMethod(excelDataValidateInfo);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }

        return meta;
    }

    private void addField(String name, String excelName, Field field, Class<?> fieldType,
            ExcelCellValidator cellValidator, ExcelCellDataTransfer cellDataTransfer) {
        if (this.classFieldMetas == null) {
            this.classFieldMetas = new ArrayList<>();
        }

        if (this.excelNameMetaMap == null) {
            this.excelNameMetaMap = new HashMap<>();
        }

        if (this.fieldNameMetaMap == null) {
            this.fieldNameMetaMap = new HashMap<>();
        }

        ClassFieldMeta fieldMeta = new ClassFieldMeta();
        fieldMeta.setName(name);
        fieldMeta.setExcelName(excelName);
        fieldMeta.setFieldType(fieldType);
        fieldMeta.setCellValidator(cellValidator);
        fieldMeta.setCellDataTransfer(cellDataTransfer);
        fieldMeta.setFiled(field);
        this.fieldNameMetaMap.put(name, fieldMeta);
        this.excelNameMetaMap.put(excelName, fieldMeta);
        this.classFieldMetas.add(fieldMeta);
    }

    /**
     * Get the number of fields in the class.
     * @return the number of fields
     */
    public int fieldCount() {
        return this.classFieldMetas.size();
    }

    /**
     * Set the position of a field in the Excel sheet.
     * @param titleName the name of the field in the Excel sheet
     * @param position the position of the field in the Excel sheet
     */
    public void setFieldPosition(String titleName, int position) {
        this.isSorted = false;
        if (this.positionFieldMetaMap == null) {
            this.positionFieldMetaMap = new HashMap<>();
        }

        this.positionFieldMetaMap.put(position, this.excelNameMetaMap.get(titleName));
    }

    /**
     * Get the metadata of a field at a given position.
     * @param position the position of the field
     * @return the metadata of the field
     */
    public ClassFieldMeta field(int position) {
        return (ClassFieldMeta) this.positionFieldMetaMap.get(position);
    }

}
