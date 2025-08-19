/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.app;

import static de.ii.xtraplatform.cql.domain.In.ID_PLACEHOLDER;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import de.ii.xtraplatform.cql.domain.Eq;
import de.ii.xtraplatform.cql.domain.Function;
import de.ii.xtraplatform.cql.domain.GeometryNode;
import de.ii.xtraplatform.cql.domain.Gt;
import de.ii.xtraplatform.cql.domain.Gte;
import de.ii.xtraplatform.cql.domain.In;
import de.ii.xtraplatform.cql.domain.IsNull;
import de.ii.xtraplatform.cql.domain.Like;
import de.ii.xtraplatform.cql.domain.LogicalOperation;
import de.ii.xtraplatform.cql.domain.Lt;
import de.ii.xtraplatform.cql.domain.Lte;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.Or;
import de.ii.xtraplatform.cql.domain.PositionNode;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.ScalarLiteral;
import de.ii.xtraplatform.cql.domain.SpatialFunction;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.cql.domain.TemporalLiteral;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.gml.domain.XMLNamespaceNormalizer;
import de.ii.xtraplatform.features.gml.infra.fes.FesAnd;
import de.ii.xtraplatform.features.gml.infra.fes.FesBBox;
import de.ii.xtraplatform.features.gml.infra.fes.FesDuring;
import de.ii.xtraplatform.features.gml.infra.fes.FesEnvelope;
import de.ii.xtraplatform.features.gml.infra.fes.FesExpression;
import de.ii.xtraplatform.features.gml.infra.fes.FesFilter;
import de.ii.xtraplatform.features.gml.infra.fes.FesLiteral;
import de.ii.xtraplatform.features.gml.infra.fes.FesNot;
import de.ii.xtraplatform.features.gml.infra.fes.FesOr;
import de.ii.xtraplatform.features.gml.infra.fes.FesPropertyIsEqualTo;
import de.ii.xtraplatform.features.gml.infra.fes.FesPropertyIsGreaterThan;
import de.ii.xtraplatform.features.gml.infra.fes.FesPropertyIsLessThan;
import de.ii.xtraplatform.features.gml.infra.fes.FesPropertyIsLike;
import de.ii.xtraplatform.features.gml.infra.fes.FesPropertyIsNull;
import de.ii.xtraplatform.features.gml.infra.fes.FesResourceId;
import de.ii.xtraplatform.features.gml.infra.fes.FesTemporalLiteral;
import de.ii.xtraplatform.features.gml.infra.fes.FesValueReference;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

public class FilterEncoderWfs {

  private static final Logger LOGGER = LoggerFactory.getLogger(FilterEncoderWfs.class);

  private final Cql cql;
  private final EpsgCrs nativeCrs;
  private final CrsTransformerFactory crsTransformerFactory;
  private final XMLNamespaceNormalizer namespaceNormalizer;
  BiFunction<List<Double>, Optional<EpsgCrs>, List<Double>> coordinatesTransformer;

  public FilterEncoderWfs(
      EpsgCrs nativeCrs,
      CrsTransformerFactory crsTransformerFactory,
      Cql cql,
      XMLNamespaceNormalizer namespaceNormalizer) {
    this.nativeCrs = nativeCrs;
    this.crsTransformerFactory = crsTransformerFactory;
    this.cql = cql;
    this.namespaceNormalizer = namespaceNormalizer;
    this.coordinatesTransformer = this::transformCoordinatesIfNecessary;
  }

  public FesFilter encode(Cql2Expression filter, FeatureSchema schema) {
    return new FesFilter(
        ImmutableList.of(
            cql.mapTemporalOperators(filter, ImmutableSet.of()).accept(new CqlToFes(schema))));
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
            featureProperty ->
                Objects.nonNull(featureProperty.getName())
                    && Objects.equals(
                        featureProperty.getName().toLowerCase(),
                        property.replaceAll(ID_PLACEHOLDER, "id").toLowerCase()))
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

  class CqlToFes implements CqlVisitor<FesExpression> {

    private final FeatureSchema schema;

    public CqlToFes(FeatureSchema schema) {
      this.schema = schema;
    }

    @Override
    public FesExpression postProcess(CqlNode node, FesExpression fesExpression) {
      return new FesFilter(List.of(fesExpression));
    }

    @Override
    public FesExpression visit(Not not, List<FesExpression> children) {
      return new FesNot(children);
    }

    @Override
    public FesExpression visit(LogicalOperation logicalOperation, List<FesExpression> children) {
      if (logicalOperation instanceof Or) {
        return new FesOr(children);
      }
      return new FesAnd(children);
    }

    @Override
    public FesExpression visit(
        BinaryScalarOperation scalarOperation, List<FesExpression> children) {
      if (scalarOperation instanceof Eq) {
        return new FesPropertyIsEqualTo((FesLiteral) children.get(0), (FesLiteral) children.get(1));
      } else if (children.size() == 2
          && children.get(0) instanceof FesValueReference
          && children.get(1) instanceof FesTemporalLiteral) {
        if (scalarOperation instanceof Lt) {
          return new FesAnd(
              ImmutableList.of(
                  new FesPropertyIsLessThan(
                      (FesLiteral) children.get(0),
                      ((FesTemporalLiteral) children.get(1)).toInstantLiteral()),
                  new FesNot(
                      ImmutableList.of(new FesPropertyIsNull((FesLiteral) children.get(0))))));
        } else if (scalarOperation instanceof Gt) {
          return new FesAnd(
              ImmutableList.of(
                  new FesPropertyIsGreaterThan(
                      (FesLiteral) children.get(0),
                      ((FesTemporalLiteral) children.get(1)).toInstantLiteral()),
                  new FesNot(
                      ImmutableList.of(new FesPropertyIsNull((FesLiteral) children.get(0))))));
        } else if (scalarOperation instanceof Lte) {
          return new FesAnd(
              ImmutableList.of(
                  new FesNot(
                      ImmutableList.of(
                          new FesPropertyIsGreaterThan(
                              (FesLiteral) children.get(0),
                              ((FesTemporalLiteral) children.get(1)).toInstantLiteral()))),
                  new FesNot(
                      ImmutableList.of(new FesPropertyIsNull((FesLiteral) children.get(0))))));
        } else if (scalarOperation instanceof Gte) {
          return new FesAnd(
              ImmutableList.of(
                  new FesNot(
                      ImmutableList.of(
                          new FesPropertyIsLessThan(
                              (FesLiteral) children.get(0),
                              ((FesTemporalLiteral) children.get(1)).toInstantLiteral()))),
                  new FesNot(
                      ImmutableList.of(new FesPropertyIsNull((FesLiteral) children.get(0))))));
        }
      }
      throw new IllegalArgumentException(
          String.format(
              "Scalar operation '%s' is not supported for WFS feature providers.",
              scalarOperation.getClass().getSimpleName()));
    }

    @Override
    public FesExpression visit(Between between, List<FesExpression> children) {
      throw new IllegalArgumentException(
          "BETWEEN predicates are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public FesExpression visit(Like like, List<FesExpression> children) {

      return new FesPropertyIsLike((FesLiteral) children.get(0), (FesLiteral) children.get(1));
    }

    @Override
    public FesExpression visit(In in, List<FesExpression> children) {
      if (children.size() != 2 || !(children.get(1) instanceof FesLiteral)) {
        throw new IllegalArgumentException(
            "IN predicates are not supported in filter expressions for WFS feature providers.");
      }
      return new FesResourceId(((FesLiteral) children.get(1)).getValue());
    }

    @Override
    public FesExpression visit(IsNull isNull, List<FesExpression> children) {
      throw new IllegalArgumentException(
          "IS NULL predicates are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public FesExpression visit(Casei casei, List<FesExpression> children) {
      throw new IllegalArgumentException(
          "Casei() is not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public FesExpression visit(Accenti accenti, List<FesExpression> children) {
      throw new IllegalArgumentException(
          "Accenti() is not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public FesExpression visit(
        de.ii.xtraplatform.cql.domain.Interval interval, List<FesExpression> children) {
      throw new IllegalArgumentException(
          "Non-trivial intervals are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public FesExpression visit(
        BinaryTemporalOperation temporalOperation, List<FesExpression> children) {

      if (children.size() == 2
          && children.get(0) instanceof FesValueReference
          && children.get(1) instanceof FesTemporalLiteral) {
        if (((FesTemporalLiteral) children.get(1)).isInstant()) {
          return new FesPropertyIsEqualTo(
              (FesValueReference) children.get(0),
              ((FesTemporalLiteral) children.get(1)).toInstantLiteral());
        }

        return new FesDuring(
            (FesValueReference) children.get(0), (FesTemporalLiteral) children.get(1));
      }

      throw new IllegalArgumentException(
          String.format(
              "Temporal operation '%s' is not supported for WFS feature providers.",
              temporalOperation.getClass().getSimpleName()));
    }

    @Override
    public FesExpression visit(
        BinarySpatialOperation spatialOperation, List<FesExpression> children) {
      if (spatialOperation.getSpatialOperator() == SpatialFunction.S_INTERSECTS
          && children.size() == 2
          && children.get(0) instanceof FesValueReference
          && children.get(1) instanceof FesEnvelope) {
        return new FesBBox(
            ((FesEnvelope) children.get(1)).getBoundingBox(),
            ((FesValueReference) children.get(0)).getValue());
      }
      throw new IllegalArgumentException(
          String.format(
              "Spatial operation '%s' is not supported for WFS feature providers.",
              spatialOperation.getClass().getSimpleName()));
    }

    @Override
    public FesExpression visit(BinaryArrayOperation arrayOperation, List<FesExpression> children) {
      throw new IllegalArgumentException(
          "Array operations are not supported for WFS feature providers.");
    }

    @Override
    public FesExpression visit(Property property, List<FesExpression> children) {
      String propertyPath =
          getPrefixedPropertyName(schema, property.getName())
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format("Property '%s' was not found.", property.getName())));

      return new FesValueReference(propertyPath);
    }

    @Override
    public FesExpression visit(ScalarLiteral scalarLiteral, List<FesExpression> children) {
      return new FesLiteral(scalarLiteral.getValue().toString());
    }

    @Override
    public FesExpression visit(TemporalLiteral temporalLiteral, List<FesExpression> children) {
      if (temporalLiteral.getType() == Instant.class) {
        Instant instant = (Instant) temporalLiteral.getValue();

        return new FesTemporalLiteral(DateTimeFormatter.ISO_INSTANT.format(instant));
      } else if (temporalLiteral.getType() == Interval.class) {
        Interval interval = (Interval) temporalLiteral.getValue();
        String start = DateTimeFormatter.ISO_INSTANT.format(interval.getStart());
        String end = DateTimeFormatter.ISO_INSTANT.format(interval.getEnd().minusSeconds(1));

        return new FesTemporalLiteral(start, end);
      } else if (temporalLiteral.getType() == LocalDate.class) {
        return new FesTemporalLiteral(((LocalDate) temporalLiteral.getValue()).toString());
      }

      throw new IllegalArgumentException(
          String.format(
              "Unsupported temporal literal type: %s", temporalLiteral.getType().getSimpleName()));
    }

    @Override
    public FesExpression visit(ArrayLiteral arrayLiteral, List<FesExpression> children) {
      if (((List<?>) arrayLiteral.getValue()).size() == 1) {
        // support id queries with a single id
        return ((CqlNode) ((List<?>) arrayLiteral.getValue()).get(0)).accept(this);
      }
      throw new IllegalArgumentException(
          "Array expressions are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public FesExpression visit(SpatialLiteral spatialLiteral, List<FesExpression> children) {
      return ((CqlNode) spatialLiteral.getValue()).accept(this);
    }

    @Override
    public FesExpression visit(PositionNode position, List<FesExpression> children) {
      throw new IllegalArgumentException(
          "Positions are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public FesExpression visit(GeometryNode geometry, List<FesExpression> children) {
      throw new IllegalArgumentException(
          "Geometries are not supported in filter expressions for WFS feature providers.");
    }

    @Override
    public FesExpression visit(Bbox bbox, List<FesExpression> children) {
      List<Double> coordinates =
          transformCoordinatesIfNecessary(bbox.getCoordinates(), bbox.getCrs());
      BoundingBox boundingBox =
          BoundingBox.of(
              coordinates.get(0),
              coordinates.get(1),
              coordinates.get(2),
              coordinates.get(3),
              nativeCrs);
      return new FesEnvelope(boundingBox);
    }

    @Override
    public FesExpression visit(Function function, List<FesExpression> children) {
      throw new IllegalArgumentException(
          String.format(
              "Functions are not supported in filter expressions for WFS feature providers. Found: %s",
              function.getName()));
    }

    @Override
    public FesExpression visit(BooleanValue2 booleanValue, List<FesExpression> children) {
      if (Objects.equals(booleanValue.getValue(), Boolean.TRUE)) {
        FesLiteral literal = new FesLiteral("1");
        return new FesPropertyIsEqualTo(literal, literal);
      }

      FesLiteral literal0 = new FesLiteral("0");
      FesLiteral literal1 = new FesLiteral("1");
      return new FesPropertyIsEqualTo(literal0, literal1);
    }
  }
}
