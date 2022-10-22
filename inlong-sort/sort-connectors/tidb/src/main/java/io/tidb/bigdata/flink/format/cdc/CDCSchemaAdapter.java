/*
 * Copyright 2021 TiDB Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.tidb.bigdata.flink.format.cdc;

import io.tidb.bigdata.cdc.Event;
import io.tidb.bigdata.cdc.RowColumn;
import io.tidb.bigdata.flink.format.cdc.RowColumnConverters.Converter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;

public class CDCSchemaAdapter implements Serializable {
  /**
   * Readable metadata
   */
  private final CDCMetadata[] metadata;

  /**
   * TypeInformation of the produced {@link RowData}.
   */
  private final TypeInformation<RowData> typeInfo;

  private static class ColumnContext implements Serializable {
    private final int index;
    private final Converter converter;

    private ColumnContext(final int index, final LogicalType type) {
      this.index = index;
      this.converter = RowColumnConverters.createConverter(type);
    }
  }

  public static class RowBuilder {
    private final Object[] objects;

    public RowBuilder(Object[] objects) {
      this.objects = objects;
    }

    public GenericRowData build(RowKind kind) {
      return GenericRowData.ofKind(kind, objects);
    }

    public GenericRowData insert() {
      return build(RowKind.INSERT);
    }

    public GenericRowData delete() {
      return build(RowKind.DELETE);
    }

    public GenericRowData updateAfter() {
      return build(RowKind.UPDATE_AFTER);
    }

    public GenericRowData updateBefore() {
      return build(RowKind.UPDATE_BEFORE);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(objects);
    }

    @Override
    public boolean equals(Object rhs) {
      if (this == rhs) {
        return true;
      }
      if (!(rhs instanceof RowBuilder)) {
        return false;
      }
      return Objects.deepEquals(objects, ((RowBuilder) rhs).objects);
    }
  }

  private final Map<String, ColumnContext> physicalFields;

  /**
   * Number of physical fields.
   */
  private final int physicalFieldCount;

  /**
   * Number of final produced fields.
   */
  private final int producedFieldCount;

  public CDCSchemaAdapter(final DataType physicalDataType,
      final Function<DataType, TypeInformation<RowData>> typeInfoFactory,
      @Nullable final CDCMetadata[] metadata) {
    this.metadata = CDCMetadata.notNull(metadata);
    final RowType physicalRowType = (RowType) physicalDataType.getLogicalType();
    final DataType producedDataType;
    this.physicalFieldCount = physicalRowType.getFieldCount();
    if (metadata != null) {
      producedDataType = DataTypeUtils.appendRowFields(physicalDataType,
          Arrays.stream(metadata).map(CDCMetadata::toField).collect(Collectors.toList()));
      this.producedFieldCount = this.physicalFieldCount + metadata.length;
    } else {
      producedDataType = physicalDataType;
      this.producedFieldCount = this.physicalFieldCount;
    }
    this.typeInfo = typeInfoFactory.apply(producedDataType);
    this.physicalFields = new HashMap<>();
    int index = 0;
    for (final RowField field : physicalRowType.getFields()) {
      physicalFields.put(field.getName(), new ColumnContext(index++, field.getType()));
    }
  }

  public RowBuilder convert(final Event event) {
    return new RowBuilder(makeRow(event));
  }

  public RowBuilder convert(final Event event, final RowColumn[] columns) {
    final Object[] objects = makeRow(event);
    for (final RowColumn column : columns) {
      final ColumnContext ctx = physicalFields.get(column.getName());
      if (ctx == null) {
        continue;
      }
      objects[ctx.index] = ctx.converter.convert(column);
    }
    return new RowBuilder(objects);
  }

  private Object[] makeRow(final Event event) {
    int metaIndex = physicalFieldCount;
    Object[] objects = new Object[producedFieldCount];
    if (metadata != null) {
      for (CDCMetadata meta : metadata) {
        objects[metaIndex++] = meta.extract(event);
      }
    }
    return objects;
  }

  public TypeInformation<RowData> getProducedType() {
    return typeInfo;
  }
}