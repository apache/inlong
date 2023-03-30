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

package org.apache.inlong.manager.common.tool.excel;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.inlong.manager.common.tool.excel.annotation.ExcelEntity;
import org.apache.inlong.manager.common.tool.excel.annotation.ExcelField;
import org.apache.inlong.manager.common.tool.excel.meta.ClassFieldMeta;
import org.apache.inlong.manager.common.tool.excel.meta.ClassMeta;
import org.apache.inlong.manager.common.tool.excel.template.ExcelImportTemplate;
import org.apache.inlong.manager.common.tool.excel.validator.ExcelBatchValidator;
import org.apache.inlong.manager.common.tool.excel.validator.ExcelCellValidator;
import org.apache.inlong.manager.common.tool.excel.validator.ExcelRowValidator;
import org.apache.poi.hssf.usermodel.DVConstraint;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDataValidation;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExcelTool {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExcelTool.class);

    /**
     * Get the excel head of the given class
     * @param e1Class the class to get the excel head from
     * @return a list of the excel head
     */
    public static <E> List<String> getExcelHead(Class<E> e1Class) {
        List<String> list = new LinkedList<>();
        Field[] fields = e1Class.getDeclaredFields();
        if (fields.length > 0) {

            for (Field field : fields) {
                field.setAccessible(true);
                ExcelField excel = field.getAnnotation(ExcelField.class);
                if (excel != null) {
                    String excelName = excel.name();
                    list.add(excelName);
                }
            }

            return list.size() > 0 ? list : null;
        } else {
            return null;
        }
    }

    /**
     * Write the given objects to an excel document
     * @param objects the objects to write
     * @param eClass the class of the objects
     * @param out the output stream to write to
     * @return true if write was successful
     * @throws IOException if there is an error writing to the output stream
     * @throws InstantiationException if there is an error instantiating the objects
     * @throws IllegalAccessException if there is an error accessing the objects
     */
    public static <E> boolean write2ExcelDoc(List<E> objects, Class<E> eClass, OutputStream out)
            throws IOException, InstantiationException, IllegalAccessException {
        if (objects != null && objects.size() != 0) {
            List<Map<String, Object>> list = write2List(objects);
            write2Excel(eClass, list, out);
            return true;
        } else {
            throw new IllegalArgumentException("DTO can not be empty!");
        }
    }

    /**
     * Write the given content to an excel document
     * @param tClass the class of the content
     * @param content the content to write
     * @param out the output stream to write to
     * @throws IOException if there is an error writing to the output stream
     * @throws IllegalAccessException if there is an error accessing the content
     * @throws InstantiationException if there is an error instantiating the content
     */
    public static <T> void write2Excel(Class<T> tClass, List<Map<String, Object>> content, OutputStream out)
            throws IOException, IllegalAccessException, InstantiationException {
        List<String> heads = getExcelHead(tClass);
        if (heads != null && !heads.isEmpty()) {
            int countColumnNum = heads.size();
            HSSFWorkbook hwb = new HSSFWorkbook();
            HSSFSheet sheet = hwb.createSheet("Sheet 1");
            HSSFRow firstRow = sheet.createRow(0);
            HSSFCell[] firstCell = new HSSFCell[countColumnNum];
            String[] names = new String[0];
            names = heads.toArray(names);

            for (int j = 0; j < countColumnNum; ++j) {
                firstCell[j] = firstRow.createCell(j);
                firstCell[j].setCellValue(new HSSFRichTextString(names[j]));
            }

            Field[] fields = tClass.getDeclaredFields();
            int validColumnId = 0;

            for (Field field : fields) {
                field.setAccessible(true);
                ExcelCellValidator<?> validator = null;
                ExcelField annotation = field.getAnnotation(ExcelField.class);
                if (annotation != null) {
                    ++validColumnId;
                    Class<?> validatorClass = annotation.validator();
                    if (validatorClass != ExcelCellValidator.class) {
                        Object vao = validatorClass.newInstance();
                        if (vao instanceof ExcelCellValidator) {
                            validator = (ExcelCellValidator<?>) vao;
                        }
                    }

                }

                if (validator != null) {
                    DVConstraint constraint = validator.constraint();
                    if (constraint != null) {
                        String[] explicitListValues = constraint.getExplicitListValues();
                        if (explicitListValues != null && explicitListValues.length > 0) {
                            StringBuilder sb = new StringBuilder(explicitListValues.length * 16);

                            for (int i = 0; i < explicitListValues.length; ++i) {
                                if (i > 0) {
                                    sb.append('\u0000');
                                }

                                sb.append(explicitListValues[i]);
                            }

                            if (sb.toString().length() > 255) {
                                throw new IllegalArgumentException("field '" + field.getName() + "' in class '"
                                        + tClass.getCanonicalName()
                                        + "' valid message length must be less than 255 characters");
                            }

                            CellRangeAddressList regions =
                                    new CellRangeAddressList(1, 255, validColumnId - 1, validColumnId - 1);
                            HSSFDataValidation dataValidation = new HSSFDataValidation(regions, constraint);
                            sheet.addValidationData(dataValidation);
                        }
                    }
                }

            }

            if (content != null) {
                for (int i = 0; i < content.size(); ++i) {
                    HSSFRow row = sheet.createRow(i + 1);
                    Map<String, Object> objectMap = content.get(i);

                    for (int i1 = 0; i1 < heads.size(); ++i1) {
                        HSSFCell xh = row.createCell(i1);
                        String title = heads.get(i1);
                        Object ov = objectMap.get(title);
                        String value = "";
                        if (ov != null) {
                            if (!(ov instanceof String)) {
                                value = String.valueOf(ov);
                            } else {
                                value = (String) ov;
                            }
                        }

                        xh.setCellValue(value);
                    }
                }
            }

            hwb.write(out);
            out.close();
            LOGGER.info("Database export succeeded");
        } else {
            throw new IllegalArgumentException("head tile can not be empty!");
        }
    }

    /**
     * Write the given objects to a list
     * @param objects the objects to write
     * @return a list of the objects
     */
    public static <E> List<Map<String, Object>> write2List(List<E> objects) {
        Map<Field, String> fieldMap = new HashMap<>(objects.size());
        Map<Field, ExcelCellDataTransfer> dataTransferEnumMap = new HashMap<>();
        E e1 = objects.get(0);
        Class<?> e1Class = e1.getClass();
        Field[] fields = e1Class.getDeclaredFields();
        if (fields.length == 0) {
            return null;
        } else {

            for (Field field : fields) {
                field.setAccessible(true);
                ExcelField excel = field.getAnnotation(ExcelField.class);
                if (excel != null) {
                    String excelName = excel.name();
                    fieldMap.put(field, excelName);
                    ExcelCellDataTransfer excelCellDataTransfer = excel.x2oTransfer();
                    dataTransferEnumMap.put(field, excelCellDataTransfer);
                }
            }

            List<Map<String, Object>> list = new ArrayList<>();

            for (E object : objects) {
                Map<String, Object> map = new HashMap<>();
                Class<?> objectClass = object.getClass();
                Field[] declaredFields = objectClass.getDeclaredFields();
                for (Field field : declaredFields) {
                    field.setAccessible(true);
                    String name = fieldMap.get(field);
                    if (name != null) {
                        Object value = null;

                        try {
                            value = field.get(object);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }

                        if (value != null) {
                            ExcelCellDataTransfer excelCellDataTransfer =
                                    dataTransferEnumMap.get(field);
                            value = excelCellDataTransfer.parse2Text(value);
                            map.put(name, value);
                        }
                    }
                }

                if (!map.isEmpty()) {
                    list.add(map);
                }
            }

            return list;
        }
    }

    /**
     * Read the given input stream and return a list of objects
     * @param is the input stream to read
     * @param eClass the class of the objects to return
     * @return a list of the objects
     * @throws IOException if there is an error reading the input stream
     * @throws IllegalAccessException if there is an error accessing the class
     * @throws InstantiationException if there is an error instantiating the class
     */
    public static <E extends ExcelImportTemplate> List<E> read(InputStream is, Class<E> eClass)
            throws IOException, IllegalAccessException, InstantiationException {
        ClassMeta<E> classMeta = ClassMeta.of(eClass);
        int fieldCount = classMeta.fieldCount();
        ExcelBatchValidator<E> batchValidator = classMeta.getBatchValidator();
        ExcelRowValidator<E> rowValidator = classMeta.getRowValidator();
        if (fieldCount == 0) {
            throw new IllegalArgumentException("There is no fields with '" + ExcelField.class.getCanonicalName()
                    + "' annotation in class  '" + eClass.getCanonicalName() + "'");
        } else {
            HSSFWorkbook hssfWorkbook = new HSSFWorkbook(is);
            List<E> result = new LinkedList<>();

            for (int numSheet = 0; numSheet < hssfWorkbook.getNumberOfSheets(); ++numSheet) {
                HSSFSheet hssfSheet = hssfWorkbook.getSheetAt(numSheet);
                if (hssfSheet != null) {
                    HSSFRow row = hssfSheet.getRow(0);
                    int valueCountInHead = 0;

                    int lastRowNum;
                    for (lastRowNum = 0; lastRowNum < fieldCount; ++lastRowNum) {
                        HSSFCell cell = row.getCell(lastRowNum, MissingCellPolicy.RETURN_BLANK_AS_NULL);
                        if (cell != null) {
                            String titleName = cell.getStringCellValue();
                            classMeta.setFieldPosition(titleName, valueCountInHead);
                            ++valueCountInHead;
                        }
                    }

                    if (valueCountInHead != 0) {
                        if (valueCountInHead != fieldCount) {
                            throw new IllegalArgumentException("the first line in sheet " + (numSheet + 1)
                                    + "doesn't match the target bean: '" + eClass.getCanonicalName() + "'");
                        }

                        lastRowNum = hssfSheet.getLastRowNum();
                        List<E> currentResult = new ArrayList<>(lastRowNum);

                        for (int rowNum = 1; rowNum <= lastRowNum; ++rowNum) {
                            boolean hasValueInRow = false;
                            HSSFRow hssfRow = hssfSheet.getRow(rowNum);
                            if (hssfRow != null) {
                                E e = eClass.newInstance();
                                boolean validate = true;
                                StringBuilder validateInfo = new StringBuilder();

                                for (int i = 0; i < fieldCount; ++i) {
                                    ClassFieldMeta fieldMeta = classMeta.field(i);
                                    ExcelCellDataTransfer cellDataTransfer = fieldMeta.getCellDataTransfer();
                                    ExcelCellValidator cellValidator = fieldMeta.getCellValidator();
                                    Field field = fieldMeta.getFiled();
                                    HSSFCell cell = hssfRow.getCell(i, MissingCellPolicy.RETURN_BLANK_AS_NULL);
                                    if (cell != null) {
                                        hasValueInRow = true;
                                        Object o = null;
                                        if (cellDataTransfer == ExcelCellDataTransfer.DATE) {
                                            CellType cellTypeEnum = cell.getCellTypeEnum();
                                            if (cellTypeEnum == CellType.STRING) {
                                                String cellValue = cell.getStringCellValue();
                                                o = cellDataTransfer.parseFromText(cellValue);
                                            } else if (cellTypeEnum == CellType.NUMERIC) {
                                                o = cell.getDateCellValue();
                                            }
                                        } else {
                                            String value = getCellStringValue(cell);
                                            o = value;
                                            if (cellDataTransfer != ExcelCellDataTransfer.NONE) {
                                                o = cellDataTransfer.parseFromText(value);
                                            }
                                        }

                                        if (o != null && cellValidator != null) {
                                            boolean validate1 = cellValidator.validate(o);
                                            if (!validate1) {
                                                validate = false;
                                                validateInfo.append(cellValidator.getInvalidTip()).append("; ");
                                            }
                                        }

                                        if (!validate) {
                                            Method validMethod = classMeta.getExcelDataValidMethod();

                                            try {
                                                classMeta.getExcelDataValidateInfoMethod().invoke(e,
                                                        validateInfo.toString());
                                                validMethod.invoke(e, false);
                                            } catch (InvocationTargetException e1) {
                                                e1.printStackTrace();
                                            }
                                        }

                                        field.set(e, o);
                                    }
                                }

                                if (hasValueInRow) {
                                    currentResult.add(e);
                                    if (rowValidator != null) {
                                        rowValidator.onNext(e);
                                    }
                                }
                            }
                        }

                        if (batchValidator != null) {
                            batchValidator.onBatch(currentResult);
                        }

                        result.addAll(currentResult);
                    }
                }
            }

            return result;
        }
    }

    private static <E> Map<String, Field> getClassFieldMap(Class<E> eClass) {
        Map<String, Field> fieldMap = new HashMap<>();
        Field[] fields = eClass.getDeclaredFields();
        if (fields.length > 0) {

            for (Field field : fields) {
                field.setAccessible(true);
                ExcelField excel = field.getAnnotation(ExcelField.class);
                if (excel != null) {
                    String excelName = excel.name();
                    fieldMap.put(excelName, field);
                }
            }

            return fieldMap;
        } else {
            throw new IllegalArgumentException("It is not have fields in class '" + eClass.getCanonicalName() + "'");
        }
    }

    private static String getCellStringValue(Cell cell) {
        String cellvalue;
        if (cell != null) {
            cell.setCellType(1);
            cellvalue = cell.getStringCellValue();
            if (!StringUtils.isEmpty(cellvalue)) {
                cellvalue = cellvalue.trim();
                cellvalue = cellvalue.replace("\n", "");
                cellvalue = cellvalue.replace("\r", "");
                cellvalue = cellvalue.replace("\\", "/");
            }
        } else {
            cellvalue = "";
        }

        return cellvalue;
    }

    public static <E> List<Map<String, Object>> read2MapList(InputStream is, Class<E> eClass)
            throws IOException, IllegalAccessException, InstantiationException {
        LinkedList<Map<String, Object>> result = new LinkedList<>();
        Map<String, Field> fieldMap = getClassFieldMap(eClass);
        if (fieldMap.size() == 0) {
            throw new IllegalArgumentException("There is no fields with '" + ExcelField.class.getCanonicalName()
                    + "' annotation in class  '" + eClass.getCanonicalName() + "'");
        } else {
            HSSFWorkbook hssfWorkbook = new HSSFWorkbook(is);

            for (int numSheet = 0; numSheet < hssfWorkbook.getNumberOfSheets(); ++numSheet) {
                HSSFSheet hssfSheet = hssfWorkbook.getSheetAt(numSheet);
                if (hssfSheet != null) {
                    Field[] fields = new Field[fieldMap.size()];
                    HSSFRow row = hssfSheet.getRow(0);
                    int valueCountInHead = 0;

                    for (int i = 0; i < fieldMap.size(); ++i) {
                        HSSFCell cell = row.getCell(i, MissingCellPolicy.RETURN_BLANK_AS_NULL);
                        if (cell != null) {
                            ++valueCountInHead;
                            String value = cell.getStringCellValue();
                            Field field = fieldMap.get(value);
                            fields[i] = field;
                        }
                    }

                    if (valueCountInHead != 0) {
                        if (valueCountInHead != fieldMap.size()) {
                            throw new IllegalArgumentException("the first line in sheet " + (numSheet + 1)
                                    + "doesn't match the target bean: '" + eClass.getCanonicalName() + "'");
                        }

                        ExcelEntity excelEntity = eClass.getAnnotation(ExcelEntity.class);
                        ExcelBatchValidator batchValidator = null;
                        ExcelRowValidator oneRowValidator = null;
                        if (excelEntity != null) {
                            Class<?> batchValidatorClass = excelEntity.batchValidator();
                            if (batchValidatorClass != ExcelBatchValidator.class) {
                                batchValidator = (ExcelBatchValidator<?>) batchValidatorClass.newInstance();
                            }

                            Class<?> oneRowValidatorClass = excelEntity.oneRowValidator();
                            if (oneRowValidatorClass != ExcelRowValidator.class) {
                                oneRowValidator = (ExcelRowValidator<?>) oneRowValidatorClass.newInstance();
                            }
                        }

                        for (int rowNum = 1; rowNum <= hssfSheet.getLastRowNum(); ++rowNum) {
                            boolean hasValueInRow = false;
                            HSSFRow hssfRow = hssfSheet.getRow(rowNum);
                            if (hssfRow != null) {
                                HashMap<String, Object> rowMap = new HashMap<>();
                                boolean valid = true;
                                StringBuilder validateInfo = new StringBuilder();

                                for (int i = 0; i < fields.length; ++i) {
                                    Field field = fields[i];
                                    field.setAccessible(true);
                                    ExcelField excel = field.getAnnotation(ExcelField.class);
                                    ExcelCellDataTransfer transfer = excel.x2oTransfer();
                                    ExcelCellValidator excelDataValidator = null;
                                    Class<?> validatorClass = excel.validator();
                                    Object o = validatorClass.newInstance();
                                    if (o instanceof ExcelCellValidator) {
                                        excelDataValidator = (ExcelCellValidator<?>) o;
                                    }

                                    HSSFCell cell = hssfRow.getCell(i, MissingCellPolicy.RETURN_BLANK_AS_NULL);
                                    if (cell != null) {
                                        hasValueInRow = true;
                                        o = null;
                                        if (transfer == ExcelCellDataTransfer.DATE) {
                                            CellType cellTypeEnum = cell.getCellTypeEnum();
                                            if (cellTypeEnum == CellType.STRING) {
                                                String cellValue = cell.getStringCellValue();
                                                o = transfer.parseFromText(cellValue);
                                            } else if (cellTypeEnum == CellType.NUMERIC) {
                                                o = cell.getDateCellValue();
                                            }
                                        } else {
                                            String value = getCellStringValue(cell);
                                            o = value;
                                            if (transfer != ExcelCellDataTransfer.NONE) {
                                                o = transfer.parseFromText(value);
                                            }
                                        }

                                        rowMap.put(field.getName(), o);
                                        if (excelDataValidator != null && !excelDataValidator.validate(o)) {
                                            valid = false;
                                            if (validateInfo.length() > 0) {
                                                validateInfo.append(",");
                                            }

                                            validateInfo.append(excelDataValidator.getInvalidTip());
                                        }
                                    }
                                }

                                if (hasValueInRow) {
                                    rowMap.put("excel_data_valid", valid);
                                    rowMap.put("excel_data_validate_info", validateInfo.toString());
                                    result.add(rowMap);
                                }

                                if (oneRowValidator != null) {
                                    oneRowValidator.onNext(rowMap);
                                }
                            }
                        }

                        if (batchValidator != null) {
                            batchValidator.onBatch(result);
                        }
                    }
                }
            }

            return result;
        }
    }
}
