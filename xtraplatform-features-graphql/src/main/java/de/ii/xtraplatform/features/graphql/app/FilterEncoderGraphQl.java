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
import de.ii.xtraplatform.cql.domain.Geometry.Coordinate;
import de.ii.xtraplatform.cql.domain.Geometry.Envelope;
import de.ii.xtraplatform.cql.domain.Geometry.LineString;
import de.ii.xtraplatform.cql.domain.Geometry.MultiLineString;
import de.ii.xtraplatform.cql.domain.Geometry.MultiPoint;
import de.ii.xtraplatform.cql.domain.Geometry.MultiPolygon;
import de.ii.xtraplatform.cql.domain.Geometry.Point;
import de.ii.xtraplatform.cql.domain.Geometry.Polygon;
import de.ii.xtraplatform.cql.domain.In;
import de.ii.xtraplatform.cql.domain.IsNull;
import de.ii.xtraplatform.cql.domain.Like;
import de.ii.xtraplatform.cql.domain.LogicalOperation;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.ScalarLiteral;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.cql.domain.TemporalLiteral;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterEncoderGraphQl {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterEncoderGraphQl.class);

  private final Cql cql;
  private final EpsgCrs nativeCrs;
  private final CrsTransformerFactory crsTransformerFactory;
  BiFunction<List<Double>, Optional<EpsgCrs>, List<Double>> coordinatesTransformer;

  public FilterEncoderGraphQl(
      EpsgCrs nativeCrs, CrsTransformerFactory crsTransformerFactory, Cql cql) {
    this.nativeCrs = nativeCrs;
    this.crsTransformerFactory = crsTransformerFactory;
    this.cql = cql;
    this.coordinatesTransformer = this::transformCoordinatesIfNecessary;
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
          "NOT predicates are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(
        LogicalOperation logicalOperation, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "AND/OR predicates are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(
        BinaryScalarOperation scalarOperation, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          String.format(
              "Scalar operation '%s' is not supported for WFS feature providers.",
              scalarOperation.getClass().getSimpleName()));
    }

    @Override
    public Map<String, String> visit(Between between, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "BETWEEN predicates are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(Like like, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "LIKE predicates are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(In in, List<Map<String, String>> children) {
      if (children.size() != 2 || !(children.get(1).containsKey("scalar"))) {
        throw new IllegalArgumentException(
            "IN predicates are not supported in filter expressions for WFS feature providers.");
      }
      LOGGER.debug("IN {}", children);

      return Map.of("identificatie", children.get(1).get("scalar"));
    }

    @Override
    public Map<String, String> visit(IsNull isNull, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "IS NULL predicates are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(Casei casei, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Casei() is not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(Accenti accenti, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Accenti() is not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(
        de.ii.xtraplatform.cql.domain.Interval interval, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Non-trivial intervals are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(
        BinaryTemporalOperation temporalOperation, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          String.format(
              "Temporal operation '%s' is not supported for WFS feature providers.",
              temporalOperation.getClass().getSimpleName()));
    }

    @Override
    public Map<String, String> visit(
        BinarySpatialOperation spatialOperation, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          String.format(
              "Spatial operation '%s' is not supported for WFS feature providers.",
              spatialOperation.getClass().getSimpleName()));
    }

    @Override
    public Map<String, String> visit(
        BinaryArrayOperation arrayOperation, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Array operations are not supported for WFS feature providers.");
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
          "Array expressions are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(
        SpatialLiteral spatialLiteral, List<Map<String, String>> children) {
      return ((CqlNode) spatialLiteral.getValue()).accept(this);
    }

    @Override
    public Map<String, String> visit(Coordinate coordinate, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Coordinates are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(Point point, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Point geometries are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(LineString lineString, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "LineString geometries are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(Polygon polygon, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Polygon geometries are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(MultiPoint multiPoint, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "MultiPoint geometries are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(
        MultiLineString multiLineString, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "MultiLineString geometries are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(
        MultiPolygon multiPolygon, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "MultiPolygon geometries are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(Envelope envelope, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          "Envelope geometries are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public Map<String, String> visit(Function function, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          String.format(
              "Functions are not supported in filter expressions for WFS feature providers. Found: %s",
              function.getName()));
    }

    @Override
    public Map<String, String> visit(
        BooleanValue2 booleanValue, List<Map<String, String>> children) {
      throw new IllegalArgumentException(
          String.format(
              "Booleans are not supported in filter expressions for WFS feature providers. Found: %s",
              booleanValue));
    }
  }
}
