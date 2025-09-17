/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.graphql.app;

import static de.ii.xtraplatform.cql.domain.In.ID_PLACEHOLDER;

import com.google.common.primitives.Doubles;
import de.ii.xtraplatform.cql.domain.Accenti;
import de.ii.xtraplatform.cql.domain.ArrayLiteral;
import de.ii.xtraplatform.cql.domain.Bbox;
import de.ii.xtraplatform.cql.domain.Between;
import de.ii.xtraplatform.cql.domain.BinaryArrayOperation;
import de.ii.xtraplatform.cql.domain.BinaryScalarOperation;
import de.ii.xtraplatform.cql.domain.BinarySpatialOperation;
import de.ii.xtraplatform.cql.domain.BinaryTemporalOperation;
import de.ii.xtraplatform.cql.domain.BooleanValue2;
import de.ii.xtraplatform.cql.domain.Casei;
import de.ii.xtraplatform.cql.domain.Cql;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.CqlVisitor;
import de.ii.xtraplatform.cql.domain.Function;
import de.ii.xtraplatform.cql.domain.GeometryNode;
import de.ii.xtraplatform.cql.domain.In;
import de.ii.xtraplatform.cql.domain.IsNull;
import de.ii.xtraplatform.cql.domain.Like;
import de.ii.xtraplatform.cql.domain.LogicalOperation;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.PositionNode;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.SIntersects;
import de.ii.xtraplatform.cql.domain.ScalarLiteral;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.cql.domain.TemporalLiteral;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.graphql.domain.GraphQlQueries;
import de.ii.xtraplatform.strings.domain.StringTemplateFilters;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterEncoderGraphQl {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterEncoderGraphQl.class);

  private final Cql cql;
  private final EpsgCrs nativeCrs;
  private final CrsTransformerFactory crsTransformerFactory;
  BiFunction<List<Double>, Optional<EpsgCrs>, List<Double>> coordinatesTransformer;
  private final GraphQlQueries queryGeneration;

  public FilterEncoderGraphQl(
      EpsgCrs nativeCrs,
      CrsTransformerFactory crsTransformerFactory,
      Cql cql,
      GraphQlQueries queryGeneration) {
    this.nativeCrs = nativeCrs;
    this.crsTransformerFactory = crsTransformerFactory;
    this.cql = cql;
    this.coordinatesTransformer = this::transformCoordinatesIfNecessary;
    this.queryGeneration = queryGeneration;
  }

  public Map<String, String> encode(Cql2Expression filter, FeatureSchema schema) {
    return filter.accept(new CqlToGraphQl(schema));
  }

  private List<Double> transformCoordinatesIfNecessary(
      List<Double> coordinates, Optional<EpsgCrs> sourceCrs) {

    if (sourceCrs.isPresent() && !Objects.equals(sourceCrs.get(), nativeCrs)) {
      Optional<CrsTransformer> transformer =
          crsTransformerFactory.getTransformer(sourceCrs.get(), nativeCrs, true);
      if (transformer.isPresent()) {
        double[] transformed =
            transformer.get().transform(Doubles.toArray(coordinates), coordinates.size() / 2, 2);
        if (Objects.isNull(transformed)) {
          throw new IllegalArgumentException(
              String.format(
                  "Filter is invalid. Coordinates cannot be transformed: %s", coordinates));
        }

        return Doubles.asList(transformed);
      }
    }
    return coordinates;
  }

  // TODO: reverse FeatureSchema for nested mappings?
  private Optional<String> getPrefixedPropertyName(FeatureSchema schema, String property) {
    return schema.getProperties().stream()
        .filter(
            featureProperty -> {
              if (Objects.nonNull(featureProperty.getName())) {
                if (Objects.equals(
                    featureProperty.getName().toLowerCase(), property.toLowerCase())) {
                  return true;
                }
                if (Objects.equals(property, ID_PLACEHOLDER) && featureProperty.isId()) {
                  return true;
                }
              }
              return false;
            })
        .map(FeatureSchema::getSourcePath)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst()
        .map(
            prefixedPath -> {
              if (prefixedPath.contains("@")) {
                return "@" + prefixedPath.replaceAll("@", "");
              }
              return prefixedPath;
            });
  }

  class CqlToGraphQl implements CqlVisitor<Map<String, String>> {

    private final FeatureSchema schema;

    public CqlToGraphQl(FeatureSchema schema) {
      this.schema = schema;
    }

    @Override
    public Map<String, String> postProcess(CqlNode node, Map<String, String> fesExpression) {
      return fesExpression;
    }

    @Override
    public Map<String, String> visit(Not not, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "NOT predicates are not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(
        LogicalOperation logicalOperation, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "AND/OR predicates are not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(
        BinaryScalarOperation scalarOperation, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          String.format(
              "Scalar operation '%s' is not supported for GraphQL feature providers.",
              scalarOperation.getClass().getSimpleName()));
    }

    @Override
    public Map<String, String> visit(Between between, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "BETWEEN predicates are not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(Like like, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "LIKE predicates are not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(In in, List<Map<String, String>> children) {
      Optional<String> template =
          queryGeneration
              .getSingle()
              .flatMap(single -> single.getArguments().getId())
              .or(queryGeneration.getCollection().getArguments()::getId);

      if (children.size() != 2 || !(children.get(1).containsKey("scalar")) || template.isEmpty()) {
        throw new IllegalArgumentException(
            "IN predicates are not supported in filter expressions for GraphQL feature providers.");
      }

      String sourcePath = children.get(0).get("property");
      String value = children.get(1).get("scalar");

      String argument =
          StringTemplateFilters.applyTemplate(
              template.get(), (Map.of("sourcePath", sourcePath, "value", value))::get);

      return fromString(argument);
    }

    @Override
    public Map<String, String> visit(IsNull isNull, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "IS NULL predicates are not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(Casei casei, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Casei() is not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(Accenti accenti, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Accenti() is not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(
        de.ii.xtraplatform.cql.domain.Interval interval, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Non-trivial intervals are not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(
        BinaryTemporalOperation temporalOperation, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          String.format(
              "Temporal operation '%s' is not supported for GraphQL feature providers.",
              temporalOperation.getClass().getSimpleName()));
    }

    @Override
    public Map<String, String> visit(
        BinarySpatialOperation spatialOperation, List<Map<String, String>> children) {
      if (spatialOperation instanceof SIntersects) {
        if (queryGeneration.getCollection().getArguments().getBbox().isEmpty()) {
          return Map.of();
        }

        String sourcePath = children.get(0).get("property");
        String wkt = asString(children.get(1), true, true);

        String argument =
            StringTemplateFilters.applyTemplate(
                queryGeneration.getCollection().getArguments().getBbox().get(),
                (Map.of("sourcePath", sourcePath, "value", wkt))::get);

        return fromString(argument);
      }
      throw new IllegalArgumentException(
          String.format(
              "Spatial operation '%s' is not supported for GraphQL feature providers.",
              spatialOperation.getClass().getSimpleName()));
    }

    @Override
    public Map<String, String> visit(
        BinaryArrayOperation arrayOperation, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Array operations are not supported for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(Property property, List<Map<String, String>> children) {
      String propertyPath =
          getPrefixedPropertyName(schema, property.getName())
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format("Property '%s' was not found.", property.getName())));

      return Map.of("property", propertyPath);
    }

    @Override
    public Map<String, String> visit(
        ScalarLiteral scalarLiteral, List<Map<String, String>> children) {
      return Map.of("scalar", scalarLiteral.getValue().toString());
    }

    @Override
    public Map<String, String> visit(
        TemporalLiteral temporalLiteral, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported temporal literal type: %s", temporalLiteral.getType().getSimpleName()));
    }

    @Override
    public Map<String, String> visit(
        ArrayLiteral arrayLiteral, List<Map<String, String>> children) {
      if (((List<?>) arrayLiteral.getValue()).size() == 1) {
        // support id queries with a single id
        return ((CqlNode) ((List<?>) arrayLiteral.getValue()).get(0)).accept(this);
      }
      throw new IllegalArgumentException(
          "Array expressions are not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(
        SpatialLiteral spatialLiteral, List<Map<String, String>> children) {
      return ((CqlNode) spatialLiteral.getValue()).accept(this);
    }

    @Override
    public Map<String, String> visit(PositionNode position, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Coordinates are not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(GeometryNode geometry, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Geometries are not supported in filter expressions for GraphQL feature providers.");
    }

    @Override
    public Map<String, String> visit(Bbox bbox, List<Map<String, String>> children) {
      if (queryGeneration.getCollection().getArguments().getGeometry().isEmpty()) {
        return Map.of();
      }
      if (!queryGeneration.getCollection().getArguments().getGeometry().get().contains("| toWkt")) {
        throw new IllegalArgumentException(
            "No valid argument geometry filter found, valid filters are [toWkt].");
      }
      List<Double> coords = transformCoordinatesIfNecessary(bbox.getCoordinates(), bbox.getCrs());
      String wkt =
          String.format(
              Locale.US,
              "POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))",
              coords.get(0),
              coords.get(1),
              coords.get(2),
              coords.get(1),
              coords.get(2),
              coords.get(3),
              coords.get(0),
              coords.get(3),
              coords.get(0),
              coords.get(1));

      String argument =
          StringTemplateFilters.applyTemplate(
              queryGeneration.getCollection().getArguments().getGeometry().get(),
              (Map.of("value", wkt))::get,
              Map.of("toWkt", java.util.function.Function.identity()));

      return fromString(argument);
    }

    @Override
    public Map<String, String> visit(Function function, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          String.format(
              "Functions are not supported in filter expressions for GraphQL feature providers. Found: %s",
              function.getName()));
    }

    @Override
    public Map<String, String> visit(
        BooleanValue2 booleanValue, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          String.format(
              "Booleans are not supported in filter expressions for GraphQL feature providers. Found: %s",
              booleanValue));
    }
  }

  static String asString(Map<String, String> obj) {
    return asString(obj, false);
  }

  static String asString(Map<String, String> obj, boolean wrap) {
    return asString(obj, wrap, false);
  }

  static String asString(Map<String, String> obj, boolean wrap, boolean quote) {
    return obj.entrySet().stream()
        .map(
            entry ->
                String.format(
                    quote ? "%s: \\\"%s\\\"" : "%s: %s", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(",", wrap ? "{" : "", wrap ? "}" : ""));
  }

  static Map<String, String> fromString(String obj) {
    return fromString(obj, false);
  }

  static Map<String, String> fromString(String obj, boolean subField) {
    String separator = subField ? " " : ":";
    String cleaned = obj.trim();

    if (!cleaned.contains(separator)) {
      return Map.of();
    }

    if (cleaned.startsWith("{") && cleaned.endsWith("}")) {
      cleaned = cleaned.substring(1, cleaned.length() - 1);
    }

    String[] split = cleaned.split(separator, 2);

    String value = split[1].trim();

    if (value.startsWith("\"") && value.endsWith("\"")) {
      value = value.substring(1, value.length() - 1);
    } else if (subField && value.startsWith("{") && value.endsWith("}")) {
      value = value.substring(1, value.length() - 1).trim();
    }

    return Map.of(split[0].trim(), value);
  }
}
