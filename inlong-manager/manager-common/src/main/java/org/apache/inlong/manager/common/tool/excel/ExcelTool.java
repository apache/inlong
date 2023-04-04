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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.inlong.manager.common.consts.InlongConstants;
import org.apache.inlong.manager.common.tool.excel.annotation.ExcelField;
import org.apache.inlong.manager.common.tool.excel.meta.ClassFieldMeta;
import org.apache.inlong.manager.common.tool.excel.meta.ClassMeta;
import org.apache.inlong.manager.common.tool.excel.template.ExcelImportTemplate;
import org.apache.inlong.manager.common.tool.excel.validator.ExcelCellValidator;
import org.apache.inlong.manager.common.util.Preconditions;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Row.MissingCellPolicy;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFDataValidation;
import org.apache.poi.xssf.usermodel.XSSFDataValidationConstraint;
import org.apache.poi.xssf.usermodel.XSSFDataValidationHelper;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

    private static final int CONSTRAINT_MAX_LENGTH = 255;
    private static final int DEFAULT_ROW_COUNT = 30;
    private static final Logger LOGGER = LoggerFactory.getLogger(ExcelTool.class);
    private static final int DEFAULT_COLUMN_WIDTH = 10000;
    private static final short DEFAULT_FONT_SIZE = 16;

    /**
     * Extracts the header row from a given class and returns it as a list of strings.
     *
     * @param e1Class the class to extract the header from
     * @return a list of header strings
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
            return list.size() > 0 ? list : Collections.emptyList();
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Writes the given content to an excel document.
     *
     * @param contents the list of contents to write
     * @param out      the output stream to write to
     * @throws IOException if there is an error writing to the output stream
     */
    public static <T> void write(List<T> contents, OutputStream out) throws IOException {
        Preconditions.expectNotEmpty(contents, "Content can not be empty!");
        T t = contents.get(0);
        Class<?> clazz = t.getClass();
        List<String> heads = extractHeader(clazz);

        Field[] fields = clazz.getDeclaredFields();
        XSSFWorkbook hwb = new XSSFWorkbook();
        XSSFSheet sheet = hwb.createSheet("Sheet 1");

        for (int index = 0; index < heads.size(); index++) {
            sheet.setColumnWidth(index, DEFAULT_COLUMN_WIDTH);
        }
        XSSFCellStyle headerCellStyle = createHeaderCellStyle(hwb);
        fillSheetHeader(sheet.createRow(0), heads, headerCellStyle);
        fillSheetValidation(clazz, sheet, fields);
        List<Map<String, String>> maps = write2List(contents);
        XSSFCellStyle contentStyle = createContentCellStyle(hwb);
        fillSheetContent(maps, heads, sheet, contentStyle);

        hwb.write(out);
        out.close();
        LOGGER.info("Database export succeeded");
    }

    private static XSSFCellStyle createHeaderCellStyle(XSSFWorkbook workbook) {
        XSSFCellStyle style = workbook.createCellStyle();
        style.setFillForegroundColor(IndexedColors.BLUE_GREY.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        XSSFFont font = workbook.createFont();
        font.setFontName("Arial");
        font.setFontHeightInPoints(DEFAULT_FONT_SIZE);
        font.setBold(true);
        font.setColor(IndexedColors.WHITE.getIndex());
        style.setFont(font);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(IndexedColors.BLACK.getIndex());
        style.setBorderTop(BorderStyle.THIN);
        style.setTopBorderColor(IndexedColors.BLACK.getIndex());
        style.setBorderLeft(BorderStyle.THIN);
        style.setLeftBorderColor(IndexedColors.BLACK.getIndex());
        style.setBorderRight(BorderStyle.THIN);
        style.setRightBorderColor(IndexedColors.BLACK.getIndex());
        return style;
    }

    private static XSSFCellStyle createContentCellStyle(XSSFWorkbook workbook) {
        XSSFCellStyle style = workbook.createCellStyle();
        style.setFillForegroundColor(IndexedColors.LIGHT_YELLOW.getIndex());
        style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        XSSFFont font = workbook.createFont();
        font.setFontName("Arial");
        font.setFontHeightInPoints(DEFAULT_FONT_SIZE);
        font.setBold(true);
        style.setFont(font);
        style.setBorderBottom(BorderStyle.THIN);
        style.setBottomBorderColor(IndexedColors.BLACK.getIndex());
        style.setBorderTop(BorderStyle.THIN);
        style.setTopBorderColor(IndexedColors.BLACK.getIndex());
        style.setBorderLeft(BorderStyle.THIN);
        style.setLeftBorderColor(IndexedColors.BLACK.getIndex());
        style.setBorderRight(BorderStyle.THIN);
        style.setRightBorderColor(IndexedColors.BLACK.getIndex());
        return style;
    }

    /**
     * Fills the output stream with the provided class meta.
     */
    public static <T> void write(Class<T> clazz, OutputStream out) throws IOException {
        List<String> heads = extractHeader(clazz);
        if (heads.isEmpty()) {
            throw new IllegalArgumentException("Excel head must not be empty!");
        }
        Field[] fields = clazz.getDeclaredFields();

        XSSFWorkbook hwb = new XSSFWorkbook();
        XSSFSheet sheet = hwb.createSheet("Sheet 1");

        for (int index = 0; index < heads.size(); index++) {
            sheet.setColumnWidth(index, DEFAULT_COLUMN_WIDTH);
        }
        XSSFCellStyle headerCellStyle = createHeaderCellStyle(hwb);
        fillSheetHeader(sheet.createRow(0), heads, headerCellStyle);
        fillSheetValidation(clazz, sheet, fields);
        //
        CellStyle contentCellStyle = createContentCellStyle(hwb);
        fillEmptySheetContent(sheet, heads.size(), contentCellStyle);

        hwb.write(out);
        out.close();
        LOGGER.info("Database export succeeded");
    }

    private static void fillEmptySheetContent(XSSFSheet sheet, int colCount, CellStyle contentCellStyle) {
        IntStream.range(1, DEFAULT_ROW_COUNT)
                .forEach(index -> {
                    XSSFRow row = sheet.createRow(index);
                    IntStream.range(0, colCount)
                            .forEach(
                                    colIndex -> {
                                        XSSFCell cell = row.createCell(colIndex);
                                        cell.setCellStyle(contentCellStyle);
                                    });
                });
    }

    /**
     * Fills the content rows of a given sheet with the provided content maps and headers.
     */
    private static void fillSheetContent(List<Map<String, String>> contents, List<String> heads, XSSFSheet sheet,
            XSSFCellStyle contentStyle) {
        Optional.ofNullable(contents)
                .ifPresent(c -> IntStream.range(0, c.size())
                        .forEach(lineId -> {
                            Map<String, String> line = contents.get(lineId);
                            Row row = sheet.createRow(lineId + 1);
                            IntStream.range(0, heads.size()).forEach(colId -> {
                                String title = heads.get(colId);
                                String ov = line.get(title);
                                String value = ov == null ? "" : ov;
                                Cell cell = row.createCell(colId);
                                cell.setCellValue(value);
                                cell.setCellStyle(contentStyle);
                            });
                        }));
    }

    /**
     * Fills the validation constraints for a given sheet based on the provided class and fields.
     *
     * @param clazz  the class to use for validation constraints
     * @param sheet  the sheet to fill with validation constraints
     * @param fields the fields to use for validation constraints
     */
    private static <T> void fillSheetValidation(Class<T> clazz, XSSFSheet sheet, Field[] fields) {
        List<Pair<String, ExcelField>> excelFields = Arrays.stream(fields).map(field -> {
            field.setAccessible(true);
            return Pair.of(field.getName(), field.getAnnotation(ExcelField.class));
        }).filter(p -> p.getRight() != null).collect(Collectors.toList());

        IntStream.range(0, excelFields.size())
                .forEach(index -> {
                    Pair<String, ExcelField> se = excelFields.get(index);
                    Class<? extends ExcelCellValidator> validator = se.getRight().validator();

                    Optional<List<String>> optionalList = Optional.ofNullable(validator)
                            .filter(v -> v != ExcelCellValidator.class)
                            .map(v -> {
                                try {
                                    return (ExcelCellValidator<?>) v.newInstance();
                                } catch (InstantiationException | IllegalAccessException e) {
                                    LOGGER.error("Can not properly create ExcelCellValidator", e);
                                    return null;
                                }
                            })
                            .map(ExcelCellValidator::constraint);
                    List<String> valueOfCol = optionalList.orElseGet(Collections::emptyList);
                    if (valueOfCol.isEmpty()) {
                        return;
                    }
                    if (String.join("\n", valueOfCol).length() > CONSTRAINT_MAX_LENGTH) {
                        throw new IllegalArgumentException(
                                "field '" + se.getLeft() + "' in class '" + clazz.getCanonicalName()
                                        + "' valid message length must be less than 255 characters");
                    }

                    CellRangeAddressList regions = new CellRangeAddressList(1, CONSTRAINT_MAX_LENGTH, index, index);
                    XSSFDataValidationHelper dvHelper = new XSSFDataValidationHelper(sheet);
                    XSSFDataValidationConstraint explicitListConstraint = (XSSFDataValidationConstraint) dvHelper
                            .createExplicitListConstraint(valueOfCol.toArray(new String[0]));
                    XSSFDataValidation dataValidation =
                            (XSSFDataValidation) dvHelper.createValidation(explicitListConstraint, regions);
                    sheet.addValidationData(dataValidation);
                });
    }

    private static void fillSheetHeader(XSSFRow row, List<String> heads, XSSFCellStyle style) {
        IntStream.range(0, heads.size()).forEach(index -> {
            XSSFCell cell = row.createCell(index);
            cell.setCellValue(new XSSFRichTextString(heads.get(index)));
            cell.setCellStyle(style);
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
        E e1 = objects.get(0);
        Class<?> e1Class = e1.getClass();
        Field[] fields = e1Class.getDeclaredFields();
        expectTrue(fields.length > 0, "No method was found in the class '" + e1Class.getSimpleName() + "'");

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

    /**
     * Read data from an Excel file and convert it to a list of objects of the specified class.
     *
     * @param is     The input stream of the Excel file.
     * @param eClass The class of the objects to be converted.
     * @param <E>    The type of the objects to be converted.
     * @return A list of objects of the specified class.
     * @throws IOException            If an I/O error occurs.
     * @throws IllegalAccessException If the class or its nullary constructor is not accessible.
     * @throws InstantiationException If the class that declares the underlying field is an interface or is abstract.
     */
    public static <E extends ExcelImportTemplate> List<E> read(InputStream is, Class<E> eClass)
            throws IOException, IllegalAccessException, InstantiationException, NoSuchMethodException {
        ClassMeta<E> classMeta = ClassMeta.of(eClass);
        int fieldCount = classMeta.fieldCount();

        expectTrue(fieldCount > 0, "The class contains at least one field with a @ExcelField annotation");
        XSSFWorkbook hssfWorkbook = new XSSFWorkbook(is);
        List<E> result = new LinkedList<>();
        for (int numSheet = 0; numSheet < hssfWorkbook.getNumberOfSheets(); ++numSheet) {
            XSSFSheet sheet = hssfWorkbook.getSheetAt(numSheet);
            if (sheet != null) {
                XSSFRow firstRow = sheet.getRow(0);

                int[] valueCountInHead = {0};
                IntStream.range(0, fieldCount)
                        .forEach(colIndex -> Optional
                                .ofNullable(firstRow.getCell(colIndex, MissingCellPolicy.CREATE_NULL_AS_BLANK))
                                .ifPresent(cell -> {
                                    classMeta.setFieldPosition(cell.getStringCellValue(), valueCountInHead[0]++);
                                }));

                expectTrue(valueCountInHead[0] == fieldCount,
                        "The first line field must be the same number of @ExcelMeta annotated fields in the class");

                int lastRowNum = sheet.getLastRowNum();
                List<E> currentResult = new ArrayList<>(lastRowNum);

                for (int rowNum = 1; rowNum <= lastRowNum; ++rowNum) {
                    XSSFRow row = sheet.getRow(rowNum);
                    if (row == null) {
                        continue;
                    }
                    E instance = eClass.newInstance();
                    StringBuilder validateInfo = new StringBuilder();
                    boolean hasValueInRow = false;
                    for (int i = 0; i < fieldCount; ++i) {
                        ClassFieldMeta fieldMeta = classMeta.field(i);
                        ExcelCellDataTransfer cellDataTransfer = fieldMeta.getCellDataTransfer();
                        XSSFCell cell = row.getCell(i, MissingCellPolicy.RETURN_BLANK_AS_NULL);
                        if (cell == null) {
                            continue;
                        }
                        hasValueInRow = true;
                        Object value = parseCellValue(cellDataTransfer, cell);
                        validateCellValue(classMeta, instance, validateInfo, fieldMeta, value);
                        fieldMeta.getFiled().set(instance, value);
                    }
                    if (hasValueInRow) {
                        currentResult.add(instance);
                        Optional.ofNullable(classMeta.getRowValidator()).ifPresent(rv -> rv.onNext(instance));
                    }
                }
                Optional.ofNullable(classMeta.getBatchValidator()).ifPresent(bv -> bv.onBatch(currentResult));
                result.addAll(currentResult);
            }
        }
        return result;
    }

    /**
     * Validate the cell value of a given field in the Excel sheet
     *
     * @param classMeta    the meta information of the Excel import template class
     * @param instance     the instance of the Excel import template class
     * @param validateInfo the StringBuilder to store validation information
     * @param fieldMeta    the meta information of the field to validate
     * @param value        the value of the field to validate
     * @throws IllegalAccessException if the field is inaccessible
     */
    private static <E extends ExcelImportTemplate> void validateCellValue(
            ClassMeta<E> classMeta,
            E instance,
            StringBuilder validateInfo,
            ClassFieldMeta fieldMeta,
            Object value) throws IllegalAccessException {
        boolean validate = true;
        ExcelCellValidator cellValidator = fieldMeta.getCellValidator();
        if (value != null && cellValidator != null) {
            boolean validate1 = cellValidator.validate(value);
            if (!validate1) {
                validate = false;
                validateInfo.append(cellValidator.getInvalidTip()).append(InlongConstants.SEMICOLON);
            }
        }

        if (!validate) {
            Method validMethod = classMeta.getExcelDataValidMethod();
            try {
                classMeta.getExcelDataValidateInfoMethod().invoke(instance, validateInfo.toString());
                validMethod.invoke(instance, false);
            } catch (InvocationTargetException e1) {
                LOGGER.error("Can not properly set value", e1);
            }
        }
    }

    /**
     * Parse the cell value of a given field in the Excel sheet
     *
     * @param cellDataTransfer the data transfer type of the cell
     * @param cell             the cell to parse
     * @return the parsed cell value
     */
    private static Object parseCellValue(ExcelCellDataTransfer cellDataTransfer, XSSFCell cell) {
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
            String value = parseCellValue(cell);
            o = value;
            if (cellDataTransfer != ExcelCellDataTransfer.NONE) {
                o = cellDataTransfer.parseFromText(value);
            }
        }
        return o;
    }

    /**
     * Parse the cell value of a given field in the Excel sheet
     *
     * @param cell the cell to parse
     * @return the parsed cell value
     */
    private static String parseCellValue(Cell cell) {
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
}
