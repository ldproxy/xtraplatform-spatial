/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import static de.ii.xtraplatform.cql.domain.In.ID_PLACEHOLDER;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.transcode.wktwkb.GeometryEncoderWkt;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.threeten.extra.Interval;

public class CqlToText implements CqlVisitor<String> {

  protected static final Set<String> KEYWORDS =
      new ImmutableSet.Builder<String>()
          .add(
              "A_EQUALS",
              "A_CONTAINS",
              "A_CONTAINEDBY",
              "A_OVERLAPS",
              "ACCENTI",
              "AND",
              "BETWEEN",
              "CASEI",
              "DATE",
              "BBOX",
              "FALSE",
              "GEOMETRYCOLLECTION",
              "IN",
              "LIKE",
              "LINESTRING",
              "MULTILINESTRING",
              "MULTIPOINT",
              "MULTIPOLYGON",
              "NOT",
              "NULL",
              "OR",
              "POINT",
              "POLYGON",
              "S_INTERSECTS",
              "S_EQUALS",
              "S_DISJOINT",
              "S_TOUCHES",
              "S_WITHIN",
              "S_OVERLAPS",
              "S_CROSSES",
              "S_CONTAINS",
              "T_AFTER",
              "T_BEFORE",
              "T_CONTAINS",
              "T_DISJOINT",
              "T_DURING",
              "T_EQUALS",
              "T_FINISHEDBY",
              "T_FINISHES",
              "T_INTERSECTS",
              "T_MEETS",
              "T_METBY",
              "T_OVERLAPPEDBY",
              "T_OVERLAPS",
              "T_STARTEDBY",
              "T_STARTS",
              "TIMESTAMP",
              "TRUE")
          .build();

  protected static final Map<Class<?>, String> LOGICAL_OPERATORS =
      new ImmutableMap.Builder<Class<?>, String>()
          .put(ImmutableAnd.class, "AND")
          .put(ImmutableOr.class, "OR")
          .put(ImmutableNot.class, "NOT")
          .build();

  protected static final Map<Class<?>, String> SCALAR_OPERATORS =
      new ImmutableMap.Builder<Class<?>, String>()
          .put(ImmutableEq.class, "=")
          .put(ImmutableNeq.class, "<>")
          .put(ImmutableGt.class, ">")
          .put(ImmutableGte.class, ">=")
          .put(ImmutableLt.class, "<")
          .put(ImmutableLte.class, "<=")
          .put(ImmutableLike.class, "LIKE")
          .put(ImmutableBetween.class, "BETWEEN")
          .put(ImmutableIn.class, "IN")
          .put(ImmutableIsNull.class, "IS NULL")
          .build();

  private final Optional<java.util.function.BiFunction<Geometry<?>, Optional<EpsgCrs>, Geometry<?>>>
      coordinatesTransformer;
  private final boolean isNestedFilter;

  public CqlToText() {
    this.coordinatesTransformer = Optional.empty();
    this.isNestedFilter = false;
  }

  public CqlToText(boolean isNestedFilter) {
    this.coordinatesTransformer = Optional.empty();
    this.isNestedFilter = isNestedFilter;
  }

  public CqlToText(
      java.util.function.BiFunction<Geometry<?>, Optional<EpsgCrs>, Geometry<?>>
          coordinatesTransformer) {
    this.coordinatesTransformer = Optional.ofNullable(coordinatesTransformer);
    this.isNestedFilter = false;
  }

  private java.util.function.Function<Geometry<?>, Geometry<?>> transformIfNecessary(
      Optional<EpsgCrs> sourceCrs) {
    return geometry -> {
      if (coordinatesTransformer.isEmpty()) {
        return geometry;
      }
      return coordinatesTransformer.get().apply(geometry, sourceCrs);
    };
  }

  private java.util.function.Function<List<Double>, List<Double>> transformIfNecessary2(
      Optional<EpsgCrs> sourceCrs) {
    return coordinates ->
        coordinatesTransformer
            .map(
                transformer ->
                    transformer.apply(
                        LineString.of(
                            PositionList.of(
                                Axes.XY, coordinates.stream().mapToDouble(d -> d).toArray())),
                        sourceCrs))
            .map(
                ls -> Arrays.stream(((LineString) ls).getValue().getCoordinates()).boxed().toList())
            .orElse(coordinates);
  }

  @Override
  public String postProcess(CqlNode node, String text) {
    // remove outer brackets
    if (node instanceof And || node instanceof Or) {
      return text.substring(1, text.length() - 1);
    }
    return text;
  }

  @Override
  public String visit(LogicalOperation logicalOperation, List<String> children) {
    String operator = LOGICAL_OPERATORS.get(logicalOperation.getClass());

    return children.stream().collect(Collectors.joining(String.format(" %s ", operator), "(", ")"));
  }

  @Override
  public String visit(Not not, List<String> children) {
    String operator = LOGICAL_OPERATORS.get(not.getClass());

    String operation = children.get(0);

    if (not.getArgs().get(0) instanceof Like) {
      String like = Like.TYPE.toUpperCase();

      return operation.replace(like, String.format("%s %s", operator, like));
    } else if (not.getArgs().get(0) instanceof IsNull) {
      String isNull = IsNull.TEXT;

      return operation.replace(isNull, "IS NOT NULL");
    } else if (not.getArgs().get(0) instanceof Between) {
      String between = Between.TYPE.toUpperCase();

      return operation.replace(between, String.format("%s %s", operator, between));
    } else if (not.getArgs().get(0) instanceof In) {
      String in = In.TYPE.toUpperCase();

      return operation.replace(in, String.format("%s %s", operator, in));
    }

    return String.format("NOT (%s)", operation);
  }

  @Override
  public String visit(BinaryScalarOperation scalarOperation, List<String> children) {
    String operator = SCALAR_OPERATORS.get(scalarOperation.getClass());
    return String.format("%s %s %s", children.get(0), operator, children.get(1));
  }

  @Override
  public String visit(Between between, List<String> children) {
    String operator = SCALAR_OPERATORS.get(between.getClass());
    return String.format(
        "%s %s %s AND %s", children.get(0), operator, children.get(1), children.get(2));
  }

  @Override
  public String visit(Like like, List<String> children) {
    String operator = SCALAR_OPERATORS.get(like.getClass());
    return String.format("%s %s %s", children.get(0), operator, children.get(1))
        .trim()
        .replace("  ", " ");
  }

  @Override
  public String visit(In in, List<String> children) {
    String operator = SCALAR_OPERATORS.get(in.getClass());
    String property = Objects.equals(children.get(0), ID_PLACEHOLDER) ? "" : children.get(0);
    String args = String.join(", ", children.subList(1, children.size()));
    return String.format("%s %s (%s)", property, operator, args.substring(1, args.length() - 1));
  }

  @Override
  public String visit(IsNull isNull, List<String> children) {
    String operator = SCALAR_OPERATORS.get(isNull.getClass());
    return String.format("%s %s", children.get(0), operator);
  }

  @Override
  public String visit(Casei casei, List<String> children) {
    return String.format("CASEI(%s)", children.get(0));
  }

  @Override
  public String visit(Accenti accenti, List<String> children) {
    return String.format("ACCENTI(%s)", children.get(0));
  }

  @Override
  public String visit(de.ii.xtraplatform.cql.domain.Interval interval, List<String> children) {
    // DATE() and TIMESTAMP() are not used in interval
    return "INTERVAL"
        + children.stream()
            .map(
                argument ->
                    argument.replace("DATE(", "").replace("TIMESTAMP(", "").replace(")", ""))
            .collect(Collectors.joining(",", "(", ")"));
  }

  @Override
  public String visit(BinaryTemporalOperation temporalOperation, List<String> children) {
    String operator = temporalOperation.getOp().toUpperCase();

    return String.format("%s(%s, %s)", operator, children.get(0), children.get(1));
  }

  @Override
  public String visit(BinarySpatialOperation spatialOperation, List<String> children) {
    String operator = spatialOperation.getOp().toUpperCase();

    return String.format("%s(%s, %s)", operator, children.get(0), children.get(1));
  }

  @Override
  public String visit(BinaryArrayOperation arrayOperation, List<String> children) {
    String operator = arrayOperation.getOp().toUpperCase();

    return String.format("%s(%s, %s)", operator, children.get(0), children.get(1));
  }

  @Override
  public String visit(GeometryNode geometry, List<String> children) {
    try {
      return new GeometryEncoderWkt(true)
          .encode(
              transformIfNecessary(geometry.getCrs().or(() -> geometry.getGeometry().getCrs()))
                  .apply(geometry.getGeometry()));
    } catch (IOException e) {
      throw new IllegalStateException("Error converting a geometry to CQL2-Text / WKT", e);
    }
  }

  @Override
  public String visit(PositionNode position, List<String> children) {
    throw new IllegalStateException("PositionNode should not be visited here");
  }

  @Override
  public String visit(Bbox bbox, List<String> children) {
    return String.format(
        "BBOX%s",
        transformIfNecessary2(bbox.getCrs()).apply(bbox.getCoordinates()).stream()
            .map(String::valueOf)
            .collect(Collectors.joining(",", "(", ")")));
  }

  @Override
  public String visit(ScalarLiteral scalarLiteral, List<String> children) {
    if (scalarLiteral.getType() == String.class) {
      return String.format("'%s'", ((String) scalarLiteral.getValue()).replaceAll("'", "''"));
    }
    return scalarLiteral.getValue().toString();
  }

  @Override
  public String visit(TemporalLiteral temporalLiteral, List<String> children) {
    if (temporalLiteral.getType() == Interval.class) {
      String start;
      String end;
      Interval interval = (Interval) temporalLiteral.getValue();
      start =
          interval.getStart().equals(Instant.MIN)
              ? ".."
              : DateTimeFormatter.ISO_INSTANT.format(interval.getStart());
      end =
          interval.getEnd().equals(Instant.MAX)
              ? ".."
              : DateTimeFormatter.ISO_INSTANT.format(interval.getEnd());
      return String.format("INTERVAL('%s','%s')", start, end);
    } else if (temporalLiteral.getType() == Instant.class) {
      Instant instant = (Instant) temporalLiteral.getValue();
      if (instant == Instant.MIN || instant == Instant.MAX) return "'..'";
      return String.format("TIMESTAMP('%s')", DateTimeFormatter.ISO_INSTANT.format(instant));
    } else if (temporalLiteral.getType() == LocalDate.class) {
      return String.format(
          "DATE('%s')", DateTimeFormatter.ISO_DATE.format((LocalDate) temporalLiteral.getValue()));
    } else if (temporalLiteral.getType() == de.ii.xtraplatform.cql.domain.Interval.class) {
      de.ii.xtraplatform.cql.domain.Interval interval =
          (de.ii.xtraplatform.cql.domain.Interval) temporalLiteral.getValue();
      return interval.getArgs().stream()
          .map(
              arg ->
                  arg.accept(this).replace("DATE(", "").replace("TIMESTAMP(", "").replace(")", ""))
          .collect(Collectors.joining(",", "INTERVAL(", ")"));
    } else if (temporalLiteral.getType() == TemporalLiteral.OPEN.class) {
      return "'..'";
    }

    throw new IllegalStateException(
        "unsupported temporal literal type: " + temporalLiteral.getType().getSimpleName());
  }

  @Override
  public String visit(ArrayLiteral arrayLiteral, List<String> children) {
    if (arrayLiteral.getValue() instanceof String) {
      return (String) arrayLiteral.getValue();
    } else {
      List<String> elements =
          ((List<Scalar>) arrayLiteral.getValue())
              .stream()
                  .map(e -> e.accept(this))
                  .map(e -> String.format("%s", e))
                  .collect(Collectors.toList());
      return String.format("(%s)", String.join(",", elements));
    }
  }

  @Override
  public String visit(SpatialLiteral spatialLiteral, List<String> children) {
    return ((CqlNode) spatialLiteral.getValue()).accept(this);
  }

  @Override
  public String visit(Property property, List<String> children) {
    if (!property.getNestedFilters().isEmpty()) {
      Map<String, Cql2Expression> nestedFilters = property.getNestedFilters();
      StringJoiner sj = new StringJoiner(".");
      for (String element : property.getPath()) {
        if (nestedFilters.containsKey(element)) {
          sj.add(
              String.format(
                  "%s[%s]",
                  addDoubleQuotes(element),
                  nestedFilters.get(element).accept(new CqlToText(true))));
        } else {
          sj.add(addDoubleQuotes(element));
        }
      }
      return sj.toString();
    } else if (isNestedFilter) {
      return addDoubleQuotes(property.getPath().get(property.getPath().size() - 1));
    } else {
      return property.getPath().stream()
          .map(this::addDoubleQuotes)
          .collect(Collectors.joining("."));
    }
  }

  private String addDoubleQuotes(String identifier) {
    if (KEYWORDS.contains(identifier.toUpperCase())) {
      return "\"" + identifier + "\"";
    }
    return identifier;
  }

  @Override
  public String visit(Function function, List<String> children) {
    return function.getName() + children.stream().collect(Collectors.joining(",", "(", ")"));
  }

  @Override
  public String visit(BooleanValue2 booleanValue, List<String> children) {
    return booleanValue.getValue().toString();
  }
}
