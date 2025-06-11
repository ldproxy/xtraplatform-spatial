/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.features.domain.SchemaBase;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

@Value.Immutable(lazyhash = true)
@Value.Style(deepImmutablesDetection = true, builder = "new", attributeBuilderDetection = true)
@JsonDeserialize(builder = ImmutableSchemaSql.Builder.class)
public interface SchemaSql extends SchemaBase<SchemaSql> {

  List<SqlRelation> getRelation();

  Optional<String> getSubDecoder();

  Map<String, String> getSubDecoderPaths();

  Map<String, PropertyTypeInfo> getSubDecoderTypes();

  Optional<String> getPrimaryKey();

  Optional<String> getSortKey();

  @Value.Default
  default boolean getSortKeyUnique() {
    return true;
  }

  List<String> getParentSortKeys();

  Optional<Cql2Expression> getFilter();

  Optional<String> getFilterString();

  Optional<String> getConstantValue();

  @Value.Default
  default boolean isExpression() {
    return false;
  }

  @JsonIgnore
  @Value.Derived
  default boolean isConstant() {
    return getConstantValue().isPresent();
  }

  @JsonIgnore
  @Override
  @Value.Auxiliary
  @Value.Derived
  default List<String> getPath() {
    List<String> path =
        getRelation().isEmpty()
            ? getSubDecoder().isPresent()
                ? List.of(
                    String.format(
                            "[%s]%s",
                            getSubDecoder().get(),
                            isExpression()
                                ? String.format(
                                    "{sql=%s}", getSubDecoderPaths().values().iterator().next())
                                : getName())
                        + getFilterString().map(filter -> "{filter=" + filter + "}").orElse(""))
                : ImmutableList.of(
                    getName()
                        + getFilterString().map(filter -> "{filter=" + filter + "}").orElse(""))
            : getRelation().stream()
                .flatMap(relation -> relation.asPath().stream()) // )
                .collect(Collectors.toList());

    // shorten paths that overlap parents, can only happen when relation is present
    if (path.size() > 1 && !getParentPath().isEmpty()) {
      int overlap = getOverlap(getParentPath(), path);

      if (overlap > 0) {
        return path.subList(overlap, path.size());
      }
    }

    return path;
  }

  static int getOverlap(List<String> path1, List<String> path2) {
    int overlap = 0;
    for (int i = 1; i <= Math.min(path1.size(), path2.size()); i++) {
      if (path1.subList(path1.size() - i, path1.size()).equals(path2.subList(0, i))) {
        overlap = i;
      }
    }
    return overlap;
  }

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default List<List<String>> getColumnPaths() {
    return getProperties().stream()
        .filter(SchemaBase::isValue)
        .map(SchemaSql::getFullPath)
        .collect(ImmutableList.toImmutableList());
  }

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default List<SchemaSql> getColumnSchemas() {
    return getProperties().stream()
        .filter(SchemaBase::isValue)
        .collect(ImmutableList.toImmutableList());
  }

  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default List<String> getSortKeys() {
    ImmutableList.Builder<String> keys = ImmutableList.builder();
    keys.addAll(getParentSortKeys());

    for (int i = getParentSortKeys().size(); i < getRelation().size(); i++) {
      if (getRelation().get(i).getSourceSortKey().isPresent()) {
        keys.add(
            String.format(
                "%s.%s",
                getRelation().get(i).getSourceContainer(),
                getRelation().get(i).getSourceSortKey().get()));
      }
    }

    if (getSortKey().isPresent()) {
      keys.add(String.format("%s.%s", getName(), getSortKey().get()));
    }

    return keys.build();
  }

  // TODO: should we do this here? can we derive it from the above?
  default List<String> getSortKeys(
      ListIterator<String> aliasesIterator, boolean onlyRelations, int keyIndexStart) {
    ImmutableList.Builder<String> keys = ImmutableList.builder();

    int keyIndex = keyIndexStart;
    SqlRelation previousRelation = null;

    for (int i = 0; i < getRelation().size(); i++) {
      SqlRelation relation = getRelation().get(i);
      String alias = aliasesIterator.next();
      if (relation.isM2N()) {
        // skip junction alias
        aliasesIterator.next();
      }

      // add keys only for main table and target tables of M:N or 1:N relations
      if (relation.getSourceSortKey().isPresent()
          && (i == 0 || previousRelation.isM2N() || previousRelation.isOne2N())) {
        String suffix = keyIndex > 0 ? "_" + keyIndex : "";
        keys.add(
            String.format("%s.%s AS SKEY%s", alias, relation.getSourceSortKey().get(), suffix));
        keyIndex++;
      }

      previousRelation = relation;
    }

    if (!onlyRelations) {
      // add key for value table
      keys.add(
          String.format(
              getSortKeyUnique()
                  ? "%s.%s AS SKEY_%d"
                  : "ROW_NUMBER() OVER (ORDER BY %s.%s) AS SKEY_%d",
              aliasesIterator.next(),
              getSortKey().get(),
              keyIndex));
    }

    return keys.build();
  }

  @Override
  @JsonIgnore
  @Value.Derived
  @Value.Auxiliary
  default List<SchemaSql> getAllNestedProperties() {
    return getProperties().stream()
        .flatMap(
            t -> {
              SchemaSql current =
                  isObject() && t.isObject()
                      ? new ImmutableSchemaSql.Builder()
                          .from(t)
                          .relation(getRelation())
                          .addAllRelation(t.getRelation())
                          .build()
                      : t;
              return Stream.concat(Stream.of(current), current.getAllNestedProperties().stream());
            })
        .collect(Collectors.toList());
  }

  @Value.Immutable
  interface PropertyTypeInfo {

    static PropertyTypeInfo of(Type t, Optional<Type> vt, boolean inArray) {
      return new ImmutablePropertyTypeInfo.Builder().type(t).valueType(vt).inArray(inArray).build();
    }

    Type getType();

    Optional<Type> getValueType();

    @Value.Default
    default boolean getInArray() {
      return false;
    }
  }
}
