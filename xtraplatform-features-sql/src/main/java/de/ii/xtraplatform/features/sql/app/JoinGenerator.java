/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.sql.domain.SchemaSql;
import de.ii.xtraplatform.features.sql.domain.SqlPath.JoinType;
import de.ii.xtraplatform.features.sql.domain.SqlQueryJoin;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.features.sql.domain.SqlRelation;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class JoinGenerator {

  public static String getJoins(
      SchemaSql table,
      List<SchemaSql> parents,
      List<String> aliases,
      FilterEncoderSql filterEncoder) {
    return getJoins(
        table, parents, aliases, Optional.empty(), Optional.empty(), false, false, filterEncoder);
  }

  public static String getJoins(
      SqlQuerySchema table, List<String> aliases, FilterEncoderSql filterEncoder) {
    return getJoins(
        table, aliases, Optional.empty(), Optional.empty(), false, false, filterEncoder);
  }

  public static String getJoins(
      SchemaSql table,
      List<SchemaSql> parents,
      List<String> aliases,
      Optional<SchemaSql> userFilterTable,
      Optional<String> userFilter,
      boolean ignoreInstanceFilter,
      boolean ignoreRelationFilters,
      FilterEncoderSql filterEncoder) {

    if (table.getRelation().isEmpty() && parents.isEmpty()) {
      return "";
    }
    ListIterator<String> aliasesIterator = aliases.listIterator();

    Optional<String> instanceFilter =
        getInstanceFilter(parents, filterEncoder, ignoreInstanceFilter);
    List<Optional<String>> relationFilters =
        getRelationFilters(table, parents, filterEncoder, ignoreRelationFilters);

    Optional<SqlRelation> userFilterRelation =
        userFilterTable.map(t -> t.getRelation().get(t.getRelation().size() - 1));
    String userFilterJoin =
        userFilter.isPresent()
            ? toJoins(userFilterRelation.get(), aliasesIterator, userFilter, instanceFilter)
                .collect(Collectors.joining(" "))
            : "";
    String userFilterTargetField = userFilterRelation.map(SqlRelation::getTargetField).orElse("");

    final int[] i = {0};
    String join =
        Stream.concat(
                parents.stream().flatMap(parent -> parent.getRelation().stream()),
                table.getRelation().stream())
            .filter(t -> !t.getTargetField().equals(userFilterTargetField))
            .flatMap(
                relation ->
                    toJoins(relation, aliasesIterator, relationFilters.get(i[0]++), instanceFilter))
            .collect(Collectors.joining(" "));
    return String.format(
        "%1$s%3$s%2$s",
        userFilterJoin, join, userFilterJoin.isEmpty() || join.isEmpty() ? "" : " ");
  }

  public static String getJoins(
      SqlQuerySchema table,
      List<String> aliases,
      Optional<SqlQuerySchema> userFilterTable,
      Optional<String> userFilter,
      boolean ignoreInstanceFilter,
      boolean ignoreRelationFilters,
      FilterEncoderSql filterEncoder) {

    if (table.getRelations().isEmpty()) {
      return "";
    }
    ListIterator<String> aliasesIterator = aliases.listIterator();

    List<Optional<String>> pathFilters =
        getPathFilters(table, filterEncoder, ignoreRelationFilters);
    Optional<String> instanceFilter = pathFilters.get(0);
    List<Optional<String>> relationFilters = pathFilters.subList(1, pathFilters.size());

    Optional<SqlQueryJoin> userFilterRelation =
        userFilterTable.map(t -> t.getRelations().get(t.getRelations().size() - 1));
    String userFilterJoin =
        userFilter.isPresent()
            ? toJoins(userFilterRelation.get(), aliasesIterator, userFilter, instanceFilter)
                .collect(Collectors.joining(" "))
            : "";
    String userFilterTargetField = userFilterRelation.map(SqlQueryJoin::getTargetField).orElse("");

    final int[] i = {0};
    String join =
        table.getRelations().stream()
            .filter(t -> !t.getTargetField().equals(userFilterTargetField))
            .flatMap(
                relation ->
                    toJoins(relation, aliasesIterator, relationFilters.get(i[0]++), instanceFilter))
            .collect(Collectors.joining(" "));
    return String.format(
        "%1$s%3$s%2$s",
        userFilterJoin, join, userFilterJoin.isEmpty() || join.isEmpty() ? "" : " ");
  }

  private static Optional<String> getInstanceFilter(
      List<SchemaSql> parents, FilterEncoderSql filterEncoder, boolean ignoreInstanceFilter) {
    return parents.isEmpty() || ignoreInstanceFilter
        ? Optional.empty()
        : parents.get(0).getFilter().map(filter -> filterEncoder.encode(filter, parents.get(0)));
  }

  private static List<Optional<String>> getRelationFilters(
      SchemaSql schema,
      List<SchemaSql> parents,
      FilterEncoderSql filterEncoder,
      boolean ignoreRelationFilters) {
    return Stream.concat(parents.stream(), Stream.of(schema))
        .flatMap(
            table ->
                ignoreRelationFilters
                    ? Stream.of(Optional.<String>empty())
                    : table.getRelation().stream()
                        .map(
                            relation ->
                                relation
                                    .getTargetFilter()
                                    .flatMap(
                                        filter ->
                                            filterEncoder.encodeRelationFilter(
                                                Optional.of(table), Optional.empty()))))
        .collect(Collectors.toList());
  }

  private static List<Optional<String>> getPathFilters(
      SqlQuerySchema schema, FilterEncoderSql filterEncoder, boolean ignorePathFilters) {
    if (ignorePathFilters) {
      return schema.asTablePath().stream()
          .map(relation -> Optional.<String>empty())
          .collect(Collectors.toList());
    }

    return IntStream.range(0, schema.asTablePath().size())
        .mapToObj(
            i ->
                schema
                    .asTablePath()
                    .get(i)
                    .getFilter()
                    .flatMap(
                        filter ->
                            filterEncoder.encodeRelationFilter2(
                                Optional.of(schema.asTablePath().subList(0, i + 1)),
                                Optional.empty())))
        .collect(Collectors.toList());
  }

  private static Stream<String> toJoins(
      SqlRelation relation,
      ListIterator<String> aliases,
      Optional<String> sqlFilter,
      Optional<String> sourceFilter) {
    List<String> joins = new ArrayList<>();

    if (relation.isM2N()) {
      String sourceAlias = aliases.next();
      String junctionAlias = aliases.next();
      String targetAlias = aliases.next();
      aliases.previous();

      joins.add(
          toJoin(
              relation.getJunction().get(),
              junctionAlias,
              relation.getJunctionSource().get(),
              relation.getSourceContainer(),
              sourceAlias,
              relation.getSourceField(),
              JoinType.INNER,
              sqlFilter,
              sourceFilter));
      joins.add(
          toJoin(
              relation.getTargetContainer(),
              targetAlias,
              relation.getTargetField(),
              relation.getJunctionSource().get(),
              junctionAlias,
              relation.getJunctionTarget().get(),
              relation.getJoinType(),
              sqlFilter,
              Optional.empty()));

    } else {
      String sourceAlias = aliases.next();
      String targetAlias = aliases.next();
      aliases.previous();

      joins.add(
          toJoin(
              relation.getTargetContainer(),
              targetAlias,
              relation.getTargetField(),
              relation.getSourceContainer(),
              sourceAlias,
              relation.getSourceField(),
              relation.getJoinType(),
              sqlFilter,
              sourceFilter));
    }

    return joins.stream();
  }

  private static Stream<String> toJoins(
      SqlQueryJoin relation,
      ListIterator<String> aliases,
      Optional<String> sqlFilter,
      Optional<String> sourceFilter) {
    List<String> joins = new ArrayList<>();

    /*TODO if (relation.isM2N()) {
      String sourceAlias = aliases.next();
      String junctionAlias = aliases.next();
      String targetAlias = aliases.next();
      aliases.previous();

      joins.add(
          toJoin(
              relation.getJunction().get(),
              junctionAlias,
              relation.getJunctionSource().get(),
              relation.getSourceContainer(),
              sourceAlias,
              relation.getSourceField(),
              JoinType.INNER,
              sqlFilter,
              sourceFilter));
      joins.add(
          toJoin(
              relation.getTargetContainer(),
              targetAlias,
              relation.getTargetField(),
              relation.getJunctionSource().get(),
              junctionAlias,
              relation.getJunctionTarget().get(),
              relation.getJoinType(),
              sqlFilter,
              Optional.empty()));

    } else {*/
    String sourceAlias = aliases.next();
    String targetAlias = aliases.next();
    aliases.previous();

    joins.add(
        toJoin(
            relation.getTarget(),
            targetAlias,
            relation.getTargetField(),
            relation.getName(),
            sourceAlias,
            relation.getSourceField(),
            relation.getJoinType(),
            sqlFilter,
            sourceFilter));
    // }

    return joins.stream();
  }

  private static String toJoin(
      String targetContainer,
      String targetAlias,
      String targetField,
      String sourceContainer,
      String sourceAlias,
      String sourceField,
      JoinType joinType,
      Optional<String> sqlFilter,
      Optional<String> sourceFilter) {
    String additionalFilter =
        sqlFilter.map(s -> " AND (" + applyNestedAlias(s, targetAlias) + ")").orElse("");
    String targetTable = targetContainer;
    String type = joinType == JoinType.INNER ? "" : joinType.name() + " ";

    if (additionalFilter.contains(FilterEncoderSql.ROW_NUMBER)) {
      String sourceFilterPart =
          sourceFilter.isPresent() ? String.format(" WHERE %s ORDER BY 1", sourceFilter.get()) : "";
      targetTable =
          String.format(
              "(SELECT A.%1$s AS A%1$s, B.*, %6$s() OVER (PARTITION BY B.%2$s ORDER BY B.%2$s) AS %6$s FROM %3$s A %7$sJOIN %4$s B ON (A.%1$s=B.%2$s)%5$s)",
              sourceField,
              targetField,
              sourceContainer,
              targetContainer,
              sourceFilterPart,
              FilterEncoderSql.ROW_NUMBER,
              type);
    }

    return String.format(
        "%7$sJOIN %1$s %2$s ON (%4$s.%5$s=%2$s.%3$s%6$s)",
        targetTable, targetAlias, targetField, sourceAlias, sourceField, additionalFilter, type);
  }

  private static String applyNestedAlias(String filter, String alias) {
    return alias.length() == 2
        ? filter.replaceAll(alias.substring(1) + "\\.", alias + ".")
        : filter;
  }
}
