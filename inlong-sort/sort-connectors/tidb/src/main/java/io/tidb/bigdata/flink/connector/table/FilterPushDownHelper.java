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

package io.tidb.bigdata.flink.connector.table;

import static io.tidb.bigdata.flink.connector.table.FilterPushDownHelper.FlinkExpression.equals;
import static io.tidb.bigdata.flink.connector.table.FilterPushDownHelper.FlinkExpression.greaterThan;
import static io.tidb.bigdata.flink.connector.table.FilterPushDownHelper.FlinkExpression.greaterThanOrEqual;
import static io.tidb.bigdata.flink.connector.table.FilterPushDownHelper.FlinkExpression.lessThan;
import static io.tidb.bigdata.flink.connector.table.FilterPushDownHelper.FlinkExpression.lessThanOrEqual;
import static io.tidb.bigdata.flink.connector.table.FilterPushDownHelper.FlinkExpression.like;
import static io.tidb.bigdata.flink.connector.table.FilterPushDownHelper.FlinkExpression.notEquals;

import com.google.common.collect.ImmutableSet;
import io.tidb.bigdata.flink.connector.source.TiDBOptions;
import io.tidb.bigdata.tidb.ClientConfig;
import io.tidb.bigdata.tidb.ClientSession;
import io.tidb.bigdata.tidb.Expressions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown.Result;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.expression.Expression;
import org.tikv.common.expression.visitor.SupportedExpressionValidator;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.types.DataType;

public class FilterPushDownHelper {

  private static final Set<FlinkExpression> COMPARISON_BINARY_FILTERS = ImmutableSet.of(
      greaterThan,
      greaterThanOrEqual,
      lessThan,
      lessThanOrEqual,
      equals,
      notEquals,
      like
  );
  static final Logger LOG = LoggerFactory.getLogger(FilterPushDownHelper.class);

  private Expression expression;
  private Map<String, DataType> nameTypeMap;
  private final ResolvedCatalogTable table;

  FilterPushDownHelper(ResolvedCatalogTable table) {
    this.table = table;
  }

  private String getRequiredProperties(String key) {
    return Preconditions.checkNotNull(table.getOptions().get(key), key + " can not be null");
  }

  private void queryNameType() {
    String databaseName = getRequiredProperties(TiDBOptions.DATABASE_NAME.key());
    String tableName = getRequiredProperties(TiDBOptions.TABLE_NAME.key());
    try (ClientSession clientSession =
        ClientSession.create(new ClientConfig(table.getOptions()))) {
      this.nameTypeMap = clientSession.getTableMust(databaseName, tableName).getColumns()
          .stream().collect(Collectors.toMap(TiColumnInfo::getName, TiColumnInfo::getType));
    } catch (Exception e) {
      throw new IllegalStateException("can not get columns", e);
    }
  }

  private Expression createExpression(List<ResolvedExpression> filters) {
    if (filters == null || filters.size() == 0) {
      return null;
    }
    if (nameTypeMap == null) {
      queryNameType();
    }
    return getExpression(filters);
  }

  private Expression getExpression(List<ResolvedExpression> resolvedExpressions) {
    return Expressions.and(resolvedExpressions.stream().map(this::getExpression)
        .filter(exp -> exp != Expressions.alwaysTrue()));
  }

  private Expression getExpression(ResolvedExpression resolvedExpression) {
    if (resolvedExpression instanceof CallExpression) {
      CallExpression callExpression = (CallExpression) resolvedExpression;
      List<ResolvedExpression> resolvedChildren = callExpression.getResolvedChildren();
      String functionName = callExpression.getFunctionName();
      Expression left = null;
      Expression right = null;
      FlinkExpression flinkExpression = FlinkExpression.fromString(functionName);
      if (COMPARISON_BINARY_FILTERS.contains(flinkExpression)) {
        left = getExpression(resolvedChildren.get(0));
        right = getExpression(resolvedChildren.get(1));
        if (left == Expressions.alwaysTrue() || right == Expressions.alwaysTrue()) {
          return Expressions.alwaysTrue();
        }
      }
      switch (flinkExpression) {
        case cast:
          // we only need column name
          return getExpression(resolvedChildren.get(0));
        case or:
          // ignore always true expression
          return Expressions.or(resolvedChildren.stream().map(this::getExpression)
              .filter(exp -> exp != Expressions.alwaysTrue()));
        case not:
          if (left == Expressions.alwaysTrue()) {
            return Expressions.alwaysTrue();
          }
          return alwaysTrueIfNotSupported(Expressions.not(left));
        case greaterThan:
          return alwaysTrueIfNotSupported(Expressions.greaterThan(left, right));
        case greaterThanOrEqual:
          return alwaysTrueIfNotSupported(Expressions.greaterEqual(left, right));
        case lessThan:
          return alwaysTrueIfNotSupported(Expressions.lessThan(left, right));
        case lessThanOrEqual:
          return alwaysTrueIfNotSupported(Expressions.lessEqual(left, right));
        case equals:
          return alwaysTrueIfNotSupported(Expressions.equal(left, right));
        case notEquals:
          return alwaysTrueIfNotSupported(Expressions.notEqual(left, right));
        case like:
          return alwaysTrueIfNotSupported(Expressions.like(left, right));
        case unresolved:
        default:
          return Expressions.alwaysTrue();
      }
    }
    if (resolvedExpression instanceof FieldReferenceExpression) {
      String name = ((FieldReferenceExpression) resolvedExpression).getName();
      return Expressions.column(name, nameTypeMap.get(name));
    }
    if (resolvedExpression instanceof ValueLiteralExpression) {
      ValueLiteralExpression valueLiteralExpression = (ValueLiteralExpression) resolvedExpression;
      Object value = valueLiteralExpression
          .getValueAs(valueLiteralExpression.getOutputDataType().getConversionClass())
          .orElseThrow(() -> new IllegalStateException("can not get value"));
      return Expressions.constant(value, null);
    }
    return Expressions.alwaysTrue();
  }

  private Expression alwaysTrueIfNotSupported(Expression expression) {
    return SupportedExpressionValidator.isSupportedExpression(expression, null)
        ? expression : Expressions.alwaysTrue();
  }

  public Result applyFilters(List<ResolvedExpression> filters) {
    LOG.debug("Flink filters: " + filters);
    if (new ClientConfig(table.getOptions()).isFilterPushDown()) {
      this.expression = createExpression(filters);
    }
    LOG.debug("TiDB expression: " + this.expression);
    return Result.of(Collections.emptyList(), filters);
  }

  public Expression getTiDBExpressions() {
    return expression;
  }

  public enum FlinkExpression {
    cast("cast"),
    or("or"),
    not("not"),
    greaterThan("greaterThan"),
    greaterThanOrEqual("greaterThanOrEqual"),
    lessThan("lessThan"),
    lessThanOrEqual("lessThanOrEqual"),
    equals("equals"),
    notEquals("notEquals"),
    like("like"),
    unresolved("_unresolved");

    private final String name;

    FlinkExpression(String name) {
      this.name = name;
    }

    public static FlinkExpression fromString(String s) {
      return Arrays.stream(values())
          .filter(flinkExpression -> flinkExpression != unresolved)
          .filter(flinkExpression -> flinkExpression.name.equals(s))
          .findFirst()
          .orElse(unresolved);
    }
  }
}