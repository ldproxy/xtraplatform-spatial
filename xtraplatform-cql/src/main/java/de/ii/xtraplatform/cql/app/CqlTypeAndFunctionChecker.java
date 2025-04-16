/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import de.ii.xtraplatform.cql.domain.Accenti;
import de.ii.xtraplatform.cql.domain.ArrayLiteral;
import de.ii.xtraplatform.cql.domain.Between;
import de.ii.xtraplatform.cql.domain.BinaryArrayOperation;
import de.ii.xtraplatform.cql.domain.BinaryScalarOperation;
import de.ii.xtraplatform.cql.domain.BinarySpatialOperation;
import de.ii.xtraplatform.cql.domain.BinaryTemporalOperation;
import de.ii.xtraplatform.cql.domain.Casei;
import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.Eq;
import de.ii.xtraplatform.cql.domain.Function;
import de.ii.xtraplatform.cql.domain.ImmutableBetween;
import de.ii.xtraplatform.cql.domain.ImmutableEq;
import de.ii.xtraplatform.cql.domain.ImmutableGt;
import de.ii.xtraplatform.cql.domain.ImmutableGte;
import de.ii.xtraplatform.cql.domain.ImmutableIn;
import de.ii.xtraplatform.cql.domain.ImmutableLike;
import de.ii.xtraplatform.cql.domain.ImmutableLt;
import de.ii.xtraplatform.cql.domain.ImmutableLte;
import de.ii.xtraplatform.cql.domain.ImmutableNeq;
import de.ii.xtraplatform.cql.domain.In;
import de.ii.xtraplatform.cql.domain.Interval;
import de.ii.xtraplatform.cql.domain.IsNull;
import de.ii.xtraplatform.cql.domain.Like;
import de.ii.xtraplatform.cql.domain.LogicalOperation;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.Scalar;
import de.ii.xtraplatform.cql.domain.ScalarLiteral;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.cql.domain.TEquals;
import de.ii.xtraplatform.cql.domain.TemporalLiteral;
import de.ii.xtraplatform.cql.infra.CqlIncompatibleTypes;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CqlTypeAndFunctionChecker extends CqlVisitorBase<Type> {

  private static final Set<Type> NUMBER = ImmutableSet.of(Type.Integer, Type.Long, Type.Double);
  private static final Set<Type> INTEGER = ImmutableSet.of(Type.Integer, Type.Long);
  private static final Set<Type> TEXT = ImmutableSet.of(Type.String);
  private static final Set<Type> BOOLEAN = ImmutableSet.of(Type.Boolean);
  private static final Set<Type> TEMPORAL =
      ImmutableSet.of(Type.LocalDate, Type.Instant, Type.Interval);
  private static final Set<Type> INSTANT = ImmutableSet.of(Type.LocalDate, Type.Instant);
  private static final Set<Type> TIMESTAMP_IN_INTERVAL = ImmutableSet.of(Type.Instant, Type.OPEN);
  private static final Set<Type> DATE_IN_INTERVAL = ImmutableSet.of(Type.LocalDate, Type.OPEN);
  private static final Set<Type> SPATIAL = ImmutableSet.of(Type.Geometry);
  private static final Set<Type> ARRAY = ImmutableSet.of(Type.List);
  private static final List<Set<Type>> SCALAR = ImmutableList.of(NUMBER, TEXT, BOOLEAN, INSTANT);
  private static final List<Set<Type>> SCALAR_ORDERED = ImmutableList.of(NUMBER, TEXT, INSTANT);
  private static final List<Set<Type>> SCALAR_ARRAY =
      ImmutableList.of(
          ImmutableSet.<Type>builder()
              .addAll(NUMBER)
              .addAll(TEXT)
              .addAll(BOOLEAN)
              .addAll(INSTANT)
              .addAll(ARRAY)
              .build());

  private static final Map<Class<?>, List<Set<Type>>> COMPATIBILITY_PREDICATES =
      new ImmutableMap.Builder<Class<?>, List<Set<Type>>>()
          .put(ImmutableEq.class, SCALAR)
          .put(ImmutableNeq.class, SCALAR)
          .put(ImmutableLt.class, SCALAR_ORDERED)
          .put(ImmutableLte.class, SCALAR_ORDERED)
          .put(ImmutableGt.class, SCALAR_ORDERED)
          .put(ImmutableGte.class, SCALAR_ORDERED)
          .put(ImmutableIn.class, SCALAR_ARRAY)
          .put(ImmutableLike.class, ImmutableList.of(TEXT))
          .put(ImmutableBetween.class, ImmutableList.of(NUMBER))
          .put(BinaryTemporalOperation.class, ImmutableList.of(TEMPORAL))
          .put(BinarySpatialOperation.class, ImmutableList.of(SPATIAL))
          .put(BinaryArrayOperation.class, ImmutableList.of(ARRAY))
          .build();

  private static final Map<String, Set<Set<Type>>> COMPATIBILITY_OTHER =
      new ImmutableMap.Builder<String, Set<Set<Type>>>()
          .put("INTERVAL", ImmutableSet.of(TIMESTAMP_IN_INTERVAL, DATE_IN_INTERVAL))
          .put("CASEI", ImmutableSet.of(TEXT))
          .put("ACCENTI", ImmutableSet.of(TEXT))
          .build();

  private static final Map<String, List<Set<Type>>> COMPATIBILITY_FUNCTION =
      new ImmutableMap.Builder<String, List<Set<Type>>>()
          .put("UPPER", ImmutableList.of(TEXT))
          .put("LOWER", ImmutableList.of(TEXT))
          .put("POSITION", ImmutableList.of(INTEGER))
          .put("DIAMETER2D", ImmutableList.of(SPATIAL))
          .put("DIAMETER3D", ImmutableList.of(SPATIAL))
          .put("ALIKE", ImmutableList.of(ImmutableSet.of(Type.List), ImmutableSet.of(Type.String)))
          .build();

  private static final Map<String, List<Set<Class<?>>>> COMPATIBILITY_FUNCTION_ARGUMENTS =
      new Builder<String, List<Set<Class<?>>>>()
          .put(
              "UPPER",
              ImmutableList.of(
                  ImmutableSet.of(
                      Property.class,
                      Function.class,
                      Accenti.class,
                      Casei.class,
                      ScalarLiteral.class)))
          .put(
              "LOWER",
              ImmutableList.of(
                  ImmutableSet.of(
                      Property.class,
                      Function.class,
                      Accenti.class,
                      Casei.class,
                      ScalarLiteral.class)))
          .put("POSITION", ImmutableList.of())
          .put("DIAMETER2D", ImmutableList.of(ImmutableSet.of(Property.class, Function.class)))
          .put("DIAMETER3D", ImmutableList.of(ImmutableSet.of(Property.class, Function.class)))
          .put(
              "ALIKE",
              ImmutableList.of(
                  ImmutableSet.of(Property.class, Function.class),
                  ImmutableSet.of(ScalarLiteral.class)))
          .build();

  private final Map<String, String> propertyTypes;
  private final Cql cql;

  public CqlTypeAndFunctionChecker(Map<String, String> propertyTypes, Cql cql) {
    this.propertyTypes = propertyTypes;
    this.cql = cql;
  }

  @Override
  public Type visit(LogicalOperation logicalOperation, List<Type> children) {
    check(children, ImmutableList.of(Type.Boolean), logicalOperation);
    return Type.Boolean;
  }

  @Override
  public Type visit(Not not, List<Type> children) {
    check(children, ImmutableList.of(Type.Boolean), not);
    return Type.Boolean;
  }

  @Override
  public Type visit(IsNull isNull, List<Type> children) {
    return Type.Boolean;
  }

  @Override
  public Type visit(BinaryScalarOperation scalarOperation, List<Type> children) {
    checkOperation(scalarOperation, children);
    return Type.Boolean;
  }

  @Override
  public Type visit(In in, List<Type> children) {
    checkOperation(in, children);
    return Type.Boolean;
  }

  @Override
  public Type visit(Like like, List<Type> children) {
    checkOperation(like, children);
    return Type.Boolean;
  }

  @Override
  public Type visit(Between between, List<Type> children) {
    checkOperation(between, children);
    return Type.Boolean;
  }

  @Override
  public Type visit(BinaryTemporalOperation temporalOperation, List<Type> children) {
    checkOperation(temporalOperation, children);
    return Type.Boolean;
  }

  @Override
  public Type visit(BinarySpatialOperation spatialOperation, List<Type> children) {
    checkOperation(spatialOperation, children);
    return Type.Boolean;
  }

  @Override
  public Type visit(BinaryArrayOperation arrayOperation, List<Type> children) {
    checkOperation(arrayOperation, children);
    return Type.Boolean;
  }

  @Override
  public Type visit(Casei casei, List<Type> children) {
    checkString(casei, children);
    return Type.String;
  }

  @Override
  public Type visit(Accenti accenti, List<Type> children) {
    checkString(accenti, children);
    return Type.String;
  }

  @Override
  public Type visit(Interval interval, List<Type> children) {
    checkInterval(interval, children);
    return Type.Interval;
  }

  @Override
  public Type visit(Function function, List<Type> children) {
    checkFunction(function, children);
    return Type.valueOf(function.getType().getSimpleName());
  }

  @Override
  public Type visit(Property property, List<Type> children) {
    String schemaType = propertyTypes.get(property.getName());
    if (Objects.nonNull(schemaType))
      switch (schemaType) {
        case "STRING":
          return Type.String;
        case "INTEGER":
          return Type.Integer;
        case "FLOAT":
          return Type.Double;
        case "BOOLEAN":
          return Type.Boolean;
        case "DATETIME":
          return Type.Instant;
        case "DATE":
          return Type.LocalDate;
        case "GEOMETRY":
          return Type.Geometry;
        case "VALUE_ARRAY":
        case "OBJECT_ARRAY":
          return Type.List;
      }
    return Type.UNKNOWN;
  }

  @Override
  public Type visit(ScalarLiteral scalarLiteral, List<Type> children) {
    return Type.valueOf(scalarLiteral.getType().getSimpleName());
  }

  @Override
  public Type visit(TemporalLiteral temporalLiteral, List<Type> children) {
    if (temporalLiteral.getType() == Interval.class)
      return ((Interval) temporalLiteral.getValue()).accept(this);
    return Type.valueOf(temporalLiteral.getType().getSimpleName());
  }

  @Override
  public Type visit(SpatialLiteral spatialLiteral, List<Type> children) {
    return Type.valueOf(spatialLiteral.getType().getSimpleName());
  }

  @Override
  public Type visit(ArrayLiteral arrayLiteral, List<Type> children) {
    return Type.valueOf(arrayLiteral.getType().getSimpleName());
  }

  private void checkOperation(CqlNode node, List<Type> types) {
    final Type firstType = types.get(0);
    if (firstType == Type.UNKNOWN) return;
    final List<Type> otherTypes = types.subList(1, types.size());

    List<Set<Type>> compatibilityLists = getCompatibilityLists(node.getClass());
    if (compatibilityLists.isEmpty())
      throw new CqlIncompatibleTypes(getText(node), firstType.schemaType(), ImmutableList.of());
    if (compatibilityLists.stream().noneMatch(list -> list.contains(firstType)))
      throw new CqlIncompatibleTypes(
          getText(node),
          firstType.schemaType(),
          asSchemaTypes(
              compatibilityLists.stream()
                  .flatMap(Collection::stream)
                  .collect(Collectors.toUnmodifiableList())));

    final List<Type> compatibleTypes =
        getCompatibilityLists(node.getClass()).stream()
            .filter(list -> list.contains(firstType))
            .flatMap(Collection::stream)
            .distinct()
            .collect(Collectors.toUnmodifiableList());
    final List<Type> expectedTypes =
        ImmutableList.<Type>builder().add(firstType).addAll(compatibleTypes).build();
    otherTypes.stream()
        .filter(type -> !expectedTypes.contains(type) && !type.equals(Type.UNKNOWN))
        .findFirst()
        .ifPresent(
            type -> {
              throw new CqlIncompatibleTypes(
                  getText(node), type.schemaType(), asSchemaTypes(expectedTypes));
            });
  }

  private void checkFunction(Function function, List<Type> types) {
    final List<Set<Class<?>>> expectedNodes =
        Objects.requireNonNullElse(
            COMPATIBILITY_FUNCTION_ARGUMENTS.get(function.getName().toUpperCase(Locale.ROOT)),
            ImmutableList.of());
    if (function.getArgs().size() != expectedNodes.size()) {
      throw new IllegalArgumentException(
          String.format(
              "Function %s expects %d argument(s), but got %d",
              function.getName(), expectedNodes.size(), function.getArgs().size()));
    }
    IntStream.range(0, function.getArgs().size())
        .forEach(
            i -> {
              CqlNode child = function.getArgs().get(i);
              Set<Class<?>> expected = expectedNodes.get(i);
              if (expected.stream()
                  .noneMatch(expectedNode -> expectedNode.isAssignableFrom(child.getClass()))) {
                throw new IllegalArgumentException(
                    String.format(
                        "Function %s expects argument %d to be a %s, but got %s",
                        function.getName(),
                        i + 1,
                        expected.stream()
                            .map(Class::getSimpleName)
                            .collect(Collectors.joining("/")),
                        child.getClass().getSimpleName().replace("Immutable", "")));
              }
            });

    final List<Set<Type>> expectedTypes =
        Objects.requireNonNullElse(
            COMPATIBILITY_FUNCTION.get(function.getName().toUpperCase(Locale.ROOT)),
            ImmutableList.of());
    IntStream.range(0, types.size())
        .forEach(
            i -> {
              Type type = types.get(i);
              Set<Type> expected = expectedTypes.get(i);
              if (expected.stream().noneMatch(expectedType -> expectedType.equals(type))) {
                throw new CqlIncompatibleTypes(
                    getText(function), i + 1, type.schemaType(), asSchemaTypesFunction(expected));
              }
            });
  }

  private void checkString(CqlNode node, List<Type> types) {
    final Set<Set<Type>> expectedTypes =
        Objects.requireNonNullElse(
            COMPATIBILITY_OTHER.get(
                node.getClass().getSimpleName().replace("Immutable", "").toUpperCase(Locale.ROOT)),
            ImmutableSet.of());
    if (expectedTypes.stream()
        .noneMatch(
            typeList ->
                types.stream()
                    .allMatch(type -> typeList.contains(type) || type.equals(Type.UNKNOWN)))) {
      throw new CqlIncompatibleTypes(
          getText(node), asSchemaTypes(types), asSchemaTypes(expectedTypes));
    }
  }

  private void checkInterval(CqlNode node, List<Type> types) {
    final Set<Set<Type>> expectedTypes =
        Objects.requireNonNullElse(
            COMPATIBILITY_OTHER.get(
                node.getClass().getSimpleName().replace("Immutable", "").toUpperCase(Locale.ROOT)),
            ImmutableSet.of());
    if (expectedTypes.stream()
        .noneMatch(
            typeList ->
                types.stream()
                    .allMatch(type -> typeList.contains(type) || type.equals(Type.UNKNOWN)))) {
      throw new CqlIncompatibleTypes(
          getText(node), asSchemaTypes(types), asSchemaTypes(expectedTypes));
    }
  }

  private List<String> asSchemaTypes(List<Type> types) {
    return types.stream().map(Type::schemaType).distinct().collect(Collectors.toUnmodifiableList());
  }

  private List<String> asSchemaTypesFunction(Set<Type> types) {
    return types.stream().map(Type::schemaType).distinct().collect(Collectors.toUnmodifiableList());
  }

  private Set<Set<String>> asSchemaTypes(Set<Set<Type>> types) {
    return types.stream()
        .map(
            typeList ->
                typeList.stream().map(Type::schemaType).collect(Collectors.toUnmodifiableSet()))
        .collect(Collectors.toUnmodifiableSet());
  }

  private void check(List<Type> types, List<Type> expectedTypes, CqlNode node) {
    types.stream()
        .filter(type -> !expectedTypes.contains(type))
        .findFirst()
        .ifPresent(
            type -> {
              throw new CqlIncompatibleTypes(
                  getText(node),
                  type.schemaType(),
                  expectedTypes.stream()
                      .map(Type::schemaType)
                      .collect(Collectors.toUnmodifiableList()));
            });
  }

  private String getText(CqlNode node) {
    if (node instanceof Function || node instanceof Casei || node instanceof Accenti) {
      return cql.write(
              Eq.of(ImmutableList.of((Scalar) node, ScalarLiteral.of("DUMMY"))), Cql.Format.TEXT)
          .replace(" = 'DUMMY'", "");
    } else if (node instanceof Interval) {
      String tmp =
          cql.write(
              TEquals.of(TemporalLiteral.of(Instant.EPOCH), (Interval) node), Cql.Format.TEXT);
      return tmp.substring(0, tmp.indexOf(",") + 1).replace(")", "");
    }
    return cql.write((Cql2Expression) node, Cql.Format.TEXT);
  }

  List<Set<Type>> getCompatibilityLists(Class<?> clazz) {
    if (COMPATIBILITY_PREDICATES.containsKey(clazz)) {
      return COMPATIBILITY_PREDICATES.get(clazz);
    }

    return COMPATIBILITY_PREDICATES.entrySet().stream()
        .filter(entry -> entry.getKey().isAssignableFrom(clazz))
        .map(Map.Entry::getValue)
        .findFirst()
        .orElse(ImmutableList.of());
  }
}
