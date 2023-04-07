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

import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.inlong.manager.common.tool.excel.annotation.ExcelEntity;
import org.apache.inlong.manager.common.tool.excel.annotation.ExcelField;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.inlong.manager.common.util.Preconditions.expectTrue;

/**
 * Utility class for working with Excel files.
 */
public class ExcelTool {

    private ExcelTool() {

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ExcelTool.class);
    private static final int DEFAULT_COLUMN_WIDTH = 10000;

    /**
     * Extracts the header row from a given class and returns it as a list of strings.
     */
    public static <E> List<String> extractHeader(Class<E> e1Class) {
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
            return !list.isEmpty() ? list : Collections.emptyList();
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Writes the given content to an Excel document.
     *
     * @param contents the list of contents to write
     * @param out      the output stream to write to
     * @throws IOException if there is an error writing to the output stream
     */
    public static <T> void write(List<T> contents, OutputStream out) throws IOException {
        Preconditions.expectNotEmpty(contents, "Content can not be empty!");
        Class<?> clazz = contents.get(0).getClass();
        List<Map<String, String>> maps = write2List(contents);
        doWrite(maps, clazz, out);
    }

    public static <T> void doWrite(List<Map<String, String>> maps, Class<T> clazz, OutputStream out)
            throws IOException {
        List<String> heads = extractHeader(clazz);
        if (heads.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one field must be marked as Excel Field by annotation @ExcelField in class " + clazz);
        }
        try (XSSFWorkbook hwb = new XSSFWorkbook()) {
            XSSFSheet sheet = hwb.createSheet("Sheet 1");

            for (int index = 0; index < heads.size(); index++) {
                sheet.setColumnWidth(index, DEFAULT_COLUMN_WIDTH);
            }
            fillSheetHeader(sheet.createRow(0), heads);
            // Fill content if data exist.
            if (CollectionUtils.isNotEmpty(maps)) {
                fillSheetContent(sheet, heads, maps);
            }

            hwb.write(out);
        }
        out.close();
        LOGGER.info("Database export succeeded");
    }

    /**
     * Fills the output stream with the provided class meta.
     */
    public static <T> void write(Class<T> clazz, OutputStream out) throws IOException {
        Preconditions.expectNotNull(clazz, "Class can not be empty!");
        doWrite(null, clazz, out);
    }

    /**
     * Fills the content rows of a given sheet with the provided content maps and headers.
     */
    private static void fillSheetContent(XSSFSheet sheet, List<String> heads, List<Map<String, String>> contents) {
        Optional.ofNullable(contents)
                .ifPresent(c -> IntStream.range(0, c.size())
                        .forEach(lineId -> {
                            Map<String, String> line = contents.get(lineId);
                            Row row = sheet.createRow(lineId + 1);
                            IntStream.range(0, heads.size()).forEach(colId -> {
                                String title = heads.get(colId);
                                String originValue = line.get(title);
                                String value = StringUtils.isNotBlank(originValue) ? originValue : "";
                                Cell cell = row.createCell(colId);
                                cell.setCellValue(value);
                            });
                        }));
    }

    private static void fillSheetHeader(XSSFRow row, List<String> heads) {
        IntStream.range(0, heads.size()).forEach(index -> {
            XSSFCell cell = row.createCell(index);
            cell.setCellValue(new XSSFRichTextString(heads.get(index)));
        });
    }

    /**
     * Convert a list of objects to a list of maps, where each map represents an object's fields and values.
     *
     * @param objects The list of objects to be converted.
     * @param <E>     The type of the objects to be converted.
     * @return A list of maps, where each map represents an object's fields and values.
     */
    public static <E> List<Map<String, String>> write2List(List<E> objects) {
        E firstInstance = objects.get(0);
        Class<?> firstClass = firstInstance.getClass();
        Field[] fields = firstClass.getDeclaredFields();
        expectTrue(fields.length > 0, "No method was found in the class '" + firstClass.getSimpleName() + "'");

        List<Triple<Field, String, ExcelCellDataTransfer>> fieldMeta = Arrays.stream(fields).map(field -> {
            field.setAccessible(true);
            return Pair.of(field, field.getAnnotation(ExcelField.class));
        }).filter(p -> p.getRight() != null)
                .map(p -> Triple.of(p.getLeft(), p.getRight().name(), p.getRight().x2oTransfer()))
                .collect(Collectors.toList());

        return objects.stream()
                .map(obj -> fieldMeta.stream().map(fm -> {
                    Object fieldValue;
                    try {
                        fieldValue = fm.getLeft().get(obj);
                    } catch (IllegalAccessException e) {
                        return null;
                    }
                    String value = fm.getRight().parse2Text(fieldValue);
                    String name = fm.getMiddle();
                    return Pair.of(name, value);
                })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue)))
                .collect(Collectors.toList());
    }

    @Data
    static class ClassFieldMeta implements Serializable {

        /**
         * The name of the field
         */
        private String name;

        /**
         * The name of the field in the Excel sheet
         */
        private String excelName;

        /**
         * The type of the field
         */
        private Class<?> fieldType;

        /**
         * The data transfer for the cell
         */
        private ExcelCellDataTransfer cellDataTransfer;

        /**
         * The field object
         */
        private transient Field filed;

        /**
         * The meta of class file.
         */
        public ClassFieldMeta() {
            // build meta
        }
    }

    /**
     * The ClassMeta class represents the metadata of a class that is used to import data from an Excel sheet.
     */
    @Data
    static class ClassMeta<T> {

        /**
         * The name of the class.
         */
        private String name;

        /**
         * The template class.
         */
        private Class<T> tClass;

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
         */
        public static <T> ClassMeta<T> of(Class<T> templateClass)
                throws InstantiationException, IllegalAccessException, NoSuchMethodException {
            ClassMeta<T> meta = new ClassMeta<>();
            meta.setTClass(templateClass);
            ExcelEntity excelEntity = templateClass.getAnnotation(ExcelEntity.class);
            if (excelEntity != null) {
                meta.name = excelEntity.name();
            }

            Field[] fields = templateClass.getDeclaredFields();
            for (Field field : fields) {
                ExcelField excelField = field.getAnnotation(ExcelField.class);
                if (excelField != null) {
                    ExcelCellDataTransfer excelCellDataTransfer = excelField.x2oTransfer();
                    meta.addField(field.getName(), excelField.name(), field, field.getType(),
                            excelCellDataTransfer);
                }
            }

            Method excelDataValid = templateClass.getMethod("setExcelDataValid", Boolean.TYPE);
            Method excelDataValidateInfo = templateClass.getMethod("setExcelDataValidate", String.class);
            meta.setExcelDataValidMethod(excelDataValid);
            meta.setExcelDataValidateInfoMethod(excelDataValidateInfo);
            return meta;
        }

        private void addField(String name, String excelName, Field field, Class<?> fieldType,
                ExcelCellDataTransfer cellDataTransfer) {
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
            fieldMeta.setCellDataTransfer(cellDataTransfer);
            fieldMeta.setFiled(field);
            this.fieldNameMetaMap.put(name, fieldMeta);
            this.excelNameMetaMap.put(excelName, fieldMeta);
            this.classFieldMetas.add(fieldMeta);
        }

        /**
         * Get the metadata of a field at a given position.
         */
        public ClassFieldMeta field(int position) {
            return this.positionFieldMetaMap.get(position);
        }

    }
}
