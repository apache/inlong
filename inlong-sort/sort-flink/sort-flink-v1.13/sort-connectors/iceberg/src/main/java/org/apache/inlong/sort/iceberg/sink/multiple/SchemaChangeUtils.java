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

package org.apache.inlong.sort.iceberg.sink.multiple;

import org.apache.inlong.sort.base.sink.TableChange;
import org.apache.inlong.sort.base.sink.TableChange.ColumnPosition;
import org.apache.inlong.sort.base.sink.TableChange.UnknownColumnChange;
import org.apache.inlong.sort.iceberg.FlinkTypeToType;

import com.google.common.collect.Sets;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaChangeUtils {

    private static final Joiner DOT = Joiner.on(".");

    /**
     * Compare two schemas and get the schema changes that happened in them.
     * TODO: currently only support add column,delete column and column type change, rename column and column position change are not supported
     *
     * @param oldSchema
     * @param newSchema
     * @return
     */
    static List<TableChange> diffSchema(Schema oldSchema, Schema newSchema) {
        List<String> oldFields = oldSchema.columns().stream().map(NestedField::name).collect(Collectors.toList());
        List<String> newFields = newSchema.columns().stream().map(NestedField::name).collect(Collectors.toList());
        Set<String> oldFieldSet = new HashSet<>(oldFields);
        Set<String> newFieldSet = new HashSet<>(newFields);

        Set<String> intersectColSet = Sets.intersection(oldFieldSet, newFieldSet);
        Set<String> colsToDelete = Sets.difference(oldFieldSet, newFieldSet);
        Set<String> colsToAdd = Sets.difference(newFieldSet, oldFieldSet);

        List<TableChange> tableChanges = new ArrayList<>();

        // step0: judge whether unknown change
        // 1.just diff two different schema can not distinguish（add + delete) vs modify
        // Example first [a, b, c] -> then delete c [a, b] -> add d [a, b, d], currently it is only judged as unknown
        // change.
        // In next version,we will judge it is [delete and add] or rename by using information extracted from DDL
        if (!colsToDelete.isEmpty() && !colsToAdd.isEmpty()) {
            tableChanges.add(new UnknownColumnChange(
                    String.format(" Old schema: [%s] and new schema: [%s], it is unknown column change",
                            oldSchema.toString(), newSchema.toString())));
            return tableChanges;
        }

        // 2.if some filed positions in new schema are not same with old schema, there is no way to deal with it.
        // This situation only is regarded as unknown column change
        if (colsToDelete.isEmpty() && colsToAdd.isEmpty() && oldFieldSet.equals(newFieldSet)
                && !oldFields.equals(newFields)) {
            tableChanges.add(
                    new UnknownColumnChange(
                            String.format(
                                    " Old schema: [%s] and new schema: [%s], they are same but some filed positions are not same."
                                            +
                                            " This situation only is regarded as unknown column change at present",
                                    oldSchema.toString(), newSchema.toString())));
            return tableChanges;
        }

        // step1: judge whether column type change
        for (String colName : intersectColSet) {
            NestedField oldField = oldSchema.findField(colName);
            NestedField newField = newSchema.findField(colName);
            if (!oldField.type().equals(newField.type()) || !oldField.doc().equals(newField.doc())) {
                tableChanges.add(
                        new TableChange.UpdateColumn(
                                new String[]{newField.name()},
                                FlinkSchemaUtil.convert(newField.type()),
                                !newField.isRequired(),
                                newField.doc()));
            }
        }

        // step2: judge whether delete column
        for (String colName : oldFields) {
            if (colsToDelete.contains(colName)) {
                tableChanges.add(
                        new TableChange.DeleteColumn(
                                new String[]{colName}));
            }
        }

        // step3: judge whether add column
        if (!colsToAdd.isEmpty()) {
            for (int i = 0; i < newFields.size(); i++) {
                String colName = newFields.get(i);
                if (colsToAdd.contains(colName)) {
                    NestedField addField = newSchema.findField(colName);
                    tableChanges.add(
                            new TableChange.AddColumn(
                                    new String[]{addField.name()},
                                    FlinkSchemaUtil.convert(addField.type()),
                                    !addField.isRequired(),
                                    addField.doc(),
                                    i == 0 ? ColumnPosition.first() : ColumnPosition.after(newFields.get(i - 1))));
                }
            }
        }
        return tableChanges;
    }

    public static void applySchemaChanges(UpdateSchema pendingUpdate, List<TableChange> tableChanges) {
        for (TableChange change : tableChanges) {
            if (change instanceof TableChange.AddColumn) {
                applyAddColumn(pendingUpdate, (TableChange.AddColumn) change);
            } else if (change instanceof TableChange.DeleteColumn) {
                applyDeleteColumn(pendingUpdate, (TableChange.DeleteColumn) change);
            } else if (change instanceof TableChange.UpdateColumn) {
                applyUpdateColumn(pendingUpdate, (TableChange.UpdateColumn) change);
            } else {
                throw new UnsupportedOperationException("Cannot apply unknown table change: " + change);
            }
        }
        pendingUpdate.commit();
    }

    public static void applyAddColumn(UpdateSchema pendingUpdate, TableChange.AddColumn add) {
        Preconditions.checkArgument(add.isNullable(),
                "Incompatible change: cannot add required column: %s", leafName(add.fieldNames()));
        Type type = add.dataType().accept(new FlinkTypeToType(RowType.of(add.dataType())));
        pendingUpdate.addColumn(parentName(add.fieldNames()), leafName(add.fieldNames()), type, add.comment());

        if (add.position() instanceof TableChange.After) {
            TableChange.After after = (TableChange.After) add.position();
            String referenceField = peerName(add.fieldNames(), after.column());
            pendingUpdate.moveAfter(DOT.join(add.fieldNames()), referenceField);

        } else if (add.position() instanceof TableChange.First) {
            pendingUpdate.moveFirst(DOT.join(add.fieldNames()));

        } else {
            Preconditions.checkArgument(add.position() == null,
                    "Cannot add '%s' at unknown position: %s", DOT.join(add.fieldNames()), add.position());
        }
    }

    public static void applyDeleteColumn(UpdateSchema pendingUpdate, TableChange.DeleteColumn delete) {
        pendingUpdate.deleteColumn(DOT.join(delete.fieldNames()));
    }

    public static void applyUpdateColumn(UpdateSchema pendingUpdate, TableChange.UpdateColumn update) {
        Type type = update.dataType().accept(new FlinkTypeToType(RowType.of(update.dataType())));
        pendingUpdate.updateColumn(DOT.join(update.fieldNames()), type.asPrimitiveType(), update.comment());
    }

    public static String leafName(String[] fieldNames) {
        Preconditions.checkArgument(fieldNames.length > 0, "Invalid field name: at least one name is required");
        return fieldNames[fieldNames.length - 1];
    }

    public static String peerName(String[] fieldNames, String fieldName) {
        if (fieldNames.length > 1) {
            String[] peerNames = Arrays.copyOf(fieldNames, fieldNames.length);
            peerNames[fieldNames.length - 1] = fieldName;
            return DOT.join(peerNames);
        }
        return fieldName;
    }

    public static String parentName(String[] fieldNames) {
        if (fieldNames.length > 1) {
            return DOT.join(Arrays.copyOfRange(fieldNames, 0, fieldNames.length - 1));
        }
        return null;
    }
}
