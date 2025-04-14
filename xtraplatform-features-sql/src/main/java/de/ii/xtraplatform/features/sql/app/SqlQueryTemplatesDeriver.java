/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.cql.domain.And;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.In;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaVisitorWithFinalizer;
import de.ii.xtraplatform.features.domain.SortKey;
import de.ii.xtraplatform.features.domain.SortKey.Direction;
import de.ii.xtraplatform.features.domain.Tuple;
import de.ii.xtraplatform.features.sql.app.SqlQueryTemplates.MetaQueryTemplate;
import de.ii.xtraplatform.features.sql.app.SqlQueryTemplates.ValueQueryTemplate;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData.QueryGeneratorSettings;
import de.ii.xtraplatform.features.sql.domain.FeatureProviderSqlData.QueryGeneratorSettings.NullOrder;
import de.ii.xtraplatform.features.sql.domain.SchemaSql;
import de.ii.xtraplatform.features.sql.domain.SqlDialect;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class SqlQueryTemplatesDeriver
    implements SchemaVisitorWithFinalizer<SchemaSql, List<ValueQueryTemplate>, SqlQueryTemplates> {

  private static final String SKEY = "SKEY";
  private static final String CSKEY = "CSKEY";
  private static final String TAB = "  ";

  private final SchemaSql queryablesSchema;
  private final SqlDialect sqlDialect;
  private final FilterEncoderSql filterEncoder;
  private final boolean computeNumberMatched;
  private final boolean computeNumberSkipped;
  private final String nullOrder;
  private final boolean geometryAsWkb;

  public SqlQueryTemplatesDeriver(
      SchemaSql queryablesSchema,
      FilterEncoderSql filterEncoder,
      SqlDialect sqlDialect,
      boolean computeNumberMatched,
      boolean computeNumberSkipped,
      Optional<NullOrder> nullOrder,
      boolean geometryAsWkb) {
    this.queryablesSchema = queryablesSchema;
    this.sqlDialect = sqlDialect;
    this.filterEncoder = filterEncoder;
    this.computeNumberMatched = computeNumberMatched;
    this.computeNumberSkipped = computeNumberSkipped;
    this.nullOrder =
        nullOrder
            .map(
                nulls ->
                    nulls == QueryGeneratorSettings.NullOrder.FIRST
                        ? " NULLS FIRST"
                        : " NULLS LAST")
            .orElse("");
    this.geometryAsWkb = geometryAsWkb;
  }

  @Override
  public List<ValueQueryTemplate> visit(
      SchemaSql schema, List<SchemaSql> parents, List<List<ValueQueryTemplate>> visitedProperties) {

    Stream<ValueQueryTemplate> current =
        schema.isObject() && schema.getProperties().stream().anyMatch(SchemaBase::isValue)
            ? Stream.of(createValueQueryTemplate(schema, parents))
            : Stream.empty();

    return Stream.concat(current, visitedProperties.stream().flatMap(Collection::stream))
        .collect(Collectors.toList());
  }

  @Override
  public SqlQueryTemplates finalize(
      SchemaSql schema, List<ValueQueryTemplate> valueQueryTemplates) {
    return new ImmutableSqlQueryTemplates.Builder()
        .metaQueryTemplate(createMetaQueryTemplate(schema))
        .valueQueryTemplates(valueQueryTemplates)
        .addAllQuerySchemas(schema.getAllObjects())
        .sortablesSchema(Optional.ofNullable(queryablesSchema))
        .build();
  }

  MetaQueryTemplate createMetaQueryTemplate(SchemaSql schema) {
    return (limit,
        offset,
        skipOffset,
        additionalSortKeys,
        cqlFilter,
        virtualTables,
        withNumberSkipped,
        withNumberReturned) -> {
      String limitAndOffsetSql = sqlDialect.applyToLimitAndOffset(limit, offset);
      String skipOffsetSql = skipOffset > 0 ? sqlDialect.applyToOffset(skipOffset) : "";
      String asIds = sqlDialect.applyToAsIds();
      Optional<String> filter = getFilter(schema, cqlFilter);
      String where = filter.isPresent() ? String.format(" WHERE %s", filter.get()) : "";

      String tableName =
          virtualTables.containsKey(schema.getName())
              ? virtualTables.get(schema.getName())
              : schema.getName();
      String table = String.format("%s A", tableName);
      String columns = "";
      for (int i = 0; i < additionalSortKeys.size(); i++) {
        SortKey sortKey = additionalSortKeys.get(i);

        columns += getSortColumn("A", sortKey, i) + ", ";
      }
      columns += String.format("A.%s AS " + SKEY, schema.getSortKey().get());
      String orderBy = getOrderBy(additionalSortKeys);
      String minMaxColumns = getMinMaxColumns(additionalSortKeys);

      String numberReturned =
          withNumberReturned
              ? String.format(
                  "SELECT %6$s, count(*) AS numberReturned FROM (SELECT %2$s FROM %1$s%5$s ORDER BY %3$s%4$s)%7$s",
                  table, columns, orderBy, limitAndOffsetSql, where, minMaxColumns, asIds)
              : sqlDialect.applyToNoTable(
                  String.format(
                      "SELECT NULL AS minKey, NULL AS maxKey, %s AS numberReturned",
                      sqlDialect.castToBigInt(0)));

      String numberMatched =
          computeNumberMatched
              ? String.format(
                  "SELECT count(*) AS numberMatched FROM (SELECT A.%2$s AS %4$s FROM %1$s A%3$s ORDER BY 1)%5$s",
                  tableName, schema.getSortKey().get(), where, SKEY, asIds)
              : sqlDialect.applyToNoTable(
                  String.format("SELECT %s AS numberMatched", sqlDialect.castToBigInt(-1)));

      String numberSkipped =
          computeNumberSkipped && withNumberSkipped
              ? String.format(
                  "SELECT CASE WHEN numberReturned = 0 THEN (SELECT count(*) AS numberSkipped FROM (SELECT %2$s FROM %1$s%5$s ORDER BY %3$s%4$s)%7$s) ELSE %6$s END AS numberSkipped FROM NR",
                  table, columns, orderBy, skipOffsetSql, where, sqlDialect.castToBigInt(-1), asIds)
              : sqlDialect.applyToNoTable(
                  String.format("SELECT %s AS numberSkipped", sqlDialect.castToBigInt(-1)));

      return String.format(
          "WITH\n%4$s%4$sNR AS (%s),\n%4$s%4$sNM AS (%s),\n%4$s%4$sNS AS (%s)\n%4$sSELECT * FROM NR, NM, NS",
          numberReturned, numberMatched, numberSkipped, TAB);
    };
  }

  private static String getSortColumn(String alias, SortKey sortKey, int i) {
    return sortKey.getField().startsWith("(")
        ? String.format("(%s.%s AS CSKEY_%d", alias, sortKey.getField().substring(1), i)
        : String.format("%s.%s AS CSKEY_%d", alias, sortKey.getField(), i);
  }

  ValueQueryTemplate createValueQueryTemplate(SchemaSql schema, List<SchemaSql> parents) {
    return (limit, offset, additionalSortKeys, filter, minMaxKeys, virtualTables) -> {
      boolean isIdFilter =
          filter
              .filter(
                  cql2Predicate -> cql2Predicate instanceof In && ((In) cql2Predicate).isIdFilter())
              .isPresent();
      List<String> aliases = AliasGenerator.getAliases(schema);

      SchemaSql rootSchema = parents.isEmpty() ? schema : parents.get(0);
      Optional<String> sqlFilter = getFilter(rootSchema, filter);
      Optional<String> whereClause =
          isIdFilter
              ? sqlFilter
              : toWhereClause(
                  aliases.get(0),
                  rootSchema.getSortKey().get(),
                  additionalSortKeys,
                  minMaxKeys,
                  sqlFilter);
      Optional<String> pagingClause =
          additionalSortKeys.isEmpty() || (limit == 0 && offset == 0)
              ? Optional.empty()
              : Optional.of(
                  String.format(
                      "%s%s",
                      limit > 0 ? sqlDialect.applyToLimit(limit) : "",
                      offset > 0 ? sqlDialect.applyToOffset(offset) : ""));

      return getTableQuery(
          schema, whereClause, pagingClause, additionalSortKeys, parents, virtualTables);
    };
  }

  private String getTableQuery(
      SchemaSql schema,
      Optional<String> whereClause,
      Optional<String> pagingClause,
      List<SortKey> additionalSortKeys,
      List<SchemaSql> parents,
      Map<String, String> virtualTables) {
    List<String> aliases = AliasGenerator.getAliases(parents, schema);
    String attributeContainerAlias = aliases.get(aliases.size() - 1);

    String mainTableName = parents.isEmpty() ? schema.getName() : parents.get(0).getName();
    if (virtualTables.containsKey(mainTableName)) {
      mainTableName = virtualTables.get(mainTableName);
    }
    String mainTableSortKey =
        parents.isEmpty() ? schema.getSortKey().get() : parents.get(0).getSortKey().get();
    String mainTable = String.format("%s %s", mainTableName, aliases.get(0));
    List<String> sortFields = getSortFields(schema, parents, aliases, additionalSortKeys);

    String columns =
        Stream.concat(
                sortFields.stream(),
                schema.accept(new OnlyReturnables()).getProperties().stream()
                    .filter(SchemaBase::isValue)
                    .map(
                        column -> {
                          String name =
                              column.isConstant()
                                  ? "'"
                                      + column.getConstantValue().get()
                                      + "'"
                                      + " AS "
                                      + column.getName()
                                  : getQualifiedColumn(attributeContainerAlias, column.getName());
                          if (column.isExpression()) {
                            return sqlDialect.applyToExpression(
                                attributeContainerAlias,
                                column.getName(),
                                column.getSubDecoderPaths(),
                                column.isSpatial());
                          }
                          if (column.isSpatial() && geometryAsWkb) {
                            return sqlDialect.applyToWkb(
                                name, column.isForcePolygonCCW(), column.shouldLinearizeCurves());
                          } else if (column.isSpatial()) {
                            return sqlDialect.applyToWkt(
                                name, column.isForcePolygonCCW(), column.shouldLinearizeCurves());
                          }
                          if (column.isTemporal()) {
                            if (column.getType() == SchemaBase.Type.DATE) {
                              return sqlDialect.applyToDate(name, column.getFormat());
                            }
                            return sqlDialect.applyToDatetime(name, column.getFormat());
                          }

                          return name;
                        }))
            .collect(Collectors.joining(", "));

    String join = JoinGenerator.getJoins(schema, parents, aliases, filterEncoder);

    String where = whereClause.map(w -> " WHERE " + w).orElse("");
    String paging = pagingClause.filter(p -> join.isEmpty()).orElse("");

    if (!join.isEmpty() && pagingClause.isPresent()) {
      String where2 = " WHERE ";
      List<String> aliasesNested = AliasGenerator.getAliases(schema, where.isEmpty() ? 1 : 2);
      String orderBy =
          IntStream.range(0, sortFields.size())
              .boxed()
              .map(
                  index -> {
                    if (index < additionalSortKeys.size()
                        && additionalSortKeys.get(index).getDirection() == Direction.DESCENDING) {
                      return sortFields.get(index) + " DESC" + nullOrder;
                    }
                    return sortFields.get(index) + nullOrder;
                  })
              .filter(sortField -> sortField.startsWith("A."))
              .map(sortField -> sortField.replace("A.", aliasesNested.get(0) + "."))
              .map(sortField -> sortField.replaceAll(" AS \\w+", ""))
              .collect(Collectors.joining(","));
      where2 +=
          String.format(
              "(A.%3$s IN (SELECT %2$s.%3$s FROM %1$s %2$s%4$s ORDER BY %5$s%6$s))",
              mainTableName,
              aliasesNested.get(0),
              mainTableSortKey,
              where
                  .replace("(A.", "(" + aliasesNested.get(0) + ".")
                  .replace(" A.", " " + aliasesNested.get(0) + "."),
              orderBy,
              pagingClause.get());

      where = where2;
    }

    String orderBy =
        IntStream.rangeClosed(1, sortFields.size())
            .boxed()
            .map(
                index -> {
                  if (index <= additionalSortKeys.size()
                      && additionalSortKeys.get(index - 1).getDirection() == Direction.DESCENDING) {
                    return index + " DESC" + nullOrder;
                  }
                  return index + nullOrder;
                })
            .collect(Collectors.joining(","));

    return String.format(
        "SELECT %s FROM %s%s%s%s ORDER BY %s%s",
        columns, mainTable, join.isEmpty() ? "" : " ", join, where, orderBy, paging);
  }

  private Optional<String> toWhereClause(
      String alias,
      String keyField,
      List<SortKey> additionalSortKeys,
      Optional<Tuple<Object, Object>> minMaxKeys,
      Optional<String> additionalFilter) {
    StringBuilder filter = new StringBuilder();

    if (minMaxKeys.isPresent() && additionalSortKeys.isEmpty()) {
      filter.append("(");
      addMinMaxFilter(filter, alias, keyField, minMaxKeys.get().first(), minMaxKeys.get().second());
      filter.append(")");
    }

    if (additionalFilter.isPresent()) {
      if (minMaxKeys.isPresent() && additionalSortKeys.isEmpty()) {
        filter.append(" AND ");
      }
      filter.append("(").append(additionalFilter.get()).append(")");
    }

    if (filter.length() == 0) {
      return Optional.empty();
    }

    return Optional.of(filter.toString());
  }

  private StringBuilder addMinMaxFilter(
      StringBuilder whereClause, String alias, String keyField, Object minKey, Object maxKey) {
    return whereClause
        .append(alias)
        .append(".")
        .append(keyField)
        .append(" >= ")
        .append(formatLiteral(minKey))
        .append(" AND ")
        .append(alias)
        .append(".")
        .append(keyField)
        .append(" <= ")
        .append(formatLiteral(maxKey));
  }

  private String formatLiteral(Object literal) {
    if (Objects.isNull(literal)) {
      return "NULL";
    }
    if (literal instanceof Number) {
      return String.valueOf(literal);
    }

    String literalString =
        literal instanceof Timestamp
            ? String.valueOf(((Timestamp) literal).toInstant())
            : String.valueOf(literal);

    return String.format("'%s'", sqlDialect.escapeString(literalString));
  }

  private List<String> getSortFields(
      SchemaSql schema,
      List<SchemaSql> parents,
      List<String> aliases,
      List<SortKey> additionalSortKeys) {

    final int[] i = {0};
    final int[] j = {0};
    Stream<String> customSortKeys =
        additionalSortKeys.stream().map(sortKey -> getSortColumn(aliases.get(0), sortKey, i[0]++));

    if (!schema.getRelation().isEmpty() || !parents.isEmpty()) {
      ListIterator<String> aliasesIterator = aliases.listIterator();

      List<String> parentSortKeys =
          parents.stream()
              .flatMap(
                  parent -> {
                    List<String> keys = parent.getSortKeys(aliasesIterator, true, j[0]);
                    j[0] += keys.size();
                    return keys.stream();
                  })
              .collect(Collectors.toList());

      return Stream.of(
              customSortKeys,
              parentSortKeys.stream(),
              schema.getSortKeys(aliasesIterator, false, parentSortKeys.size()).stream())
          .flatMap(s -> s)
          .collect(Collectors.toList());
    } else {
      return Stream.concat(
              customSortKeys,
              Stream.of(
                  String.format(
                      schema.getSortKeyUnique()
                          ? "%s.%s AS SKEY"
                          : "ROW_NUMBER() OVER (ORDER BY %s.%s) AS SKEY",
                      aliases.get(0),
                      schema.getSortKey().get())))
          .collect(Collectors.toList());
    }
  }

  private String getQualifiedColumn(String table, String column) {
    return column.contains("(")
        ? column.replaceAll("((?:\\w+\\()+)(\\w+)((?:\\))+)", "$1" + table + ".$2$3 AS $2")
        : String.format("%s.%s", table, column);
  }

  private Optional<String> getFilter(SchemaSql schema, Optional<Cql2Expression> userFilter) {
    if (schema.getFilter().isEmpty() && userFilter.isEmpty()) {
      return Optional.empty();
    }
    SchemaSql queryables = Objects.requireNonNullElse(queryablesSchema, schema);

    if (schema.getFilter().isPresent() && schema.getRelation().isEmpty() && userFilter.isEmpty()) {
      return Optional.of(filterEncoder.encode(schema.getFilter().get(), queryables));
    }
    if (schema.getFilter().isEmpty() && schema.getRelation().isEmpty() && userFilter.isPresent()) {
      return Optional.of(filterEncoder.encode(userFilter.get(), queryables));
    }
    if (schema.getFilter().isPresent()
        && schema.getRelation().isEmpty()
        && userFilter.isPresent()) {
      Cql2Expression mergedFilter = And.of(schema.getFilter().get(), userFilter.get());

      return Optional.of(filterEncoder.encode(mergedFilter, queryables));
    }
    // TODO what to do, if schema.getRelation().isPresent() ?

    return Optional.empty();
  }

  private String getOrderBy(List<SortKey> sortKeys) {
    String orderBy = "";

    for (int i = 0; i < sortKeys.size(); i++) {
      SortKey sortKey = sortKeys.get(i);

      orderBy +=
          CSKEY
              + "_"
              + i
              + (sortKey.getDirection() == Direction.DESCENDING ? " DESC" : "")
              + nullOrder
              + ", ";
    }

    orderBy += SKEY;

    return orderBy;
  }

  private String getMinMaxColumns(List<SortKey> sortKeys) {
    String minMaxKeys = "";

    if (!sortKeys.isEmpty()) {
      minMaxKeys += "NULL AS minKey, ";
      minMaxKeys += "NULL AS maxKey";
    } else {
      minMaxKeys += "MIN(" + SKEY + ") AS minKey, ";
      minMaxKeys += "MAX(" + SKEY + ") AS maxKey";
    }

    return minMaxKeys;
  }
}
