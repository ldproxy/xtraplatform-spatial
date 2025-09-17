/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.infra;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.cql.app.CqlVisitorPropertyPrefix;
import de.ii.xtraplatform.cql.domain.Accenti;
import de.ii.xtraplatform.cql.domain.And;
import de.ii.xtraplatform.cql.domain.ArrayFunction;
import de.ii.xtraplatform.cql.domain.ArrayLiteral;
import de.ii.xtraplatform.cql.domain.Between;
import de.ii.xtraplatform.cql.domain.BinaryArrayOperation;
import de.ii.xtraplatform.cql.domain.BinaryScalarOperation;
import de.ii.xtraplatform.cql.domain.BinarySpatialOperation;
import de.ii.xtraplatform.cql.domain.BinaryTemporalOperation;
import de.ii.xtraplatform.cql.domain.BooleanValue2;
import de.ii.xtraplatform.cql.domain.Casei;
import de.ii.xtraplatform.cql.domain.ComparisonOperator;
import de.ii.xtraplatform.cql.domain.Cql2Expression;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.CqlParseException;
import de.ii.xtraplatform.cql.domain.Function;
import de.ii.xtraplatform.cql.domain.GeometryNode;
import de.ii.xtraplatform.cql.domain.ImmutableBbox;
import de.ii.xtraplatform.cql.domain.ImmutableBetween;
import de.ii.xtraplatform.cql.domain.ImmutableEq;
import de.ii.xtraplatform.cql.domain.ImmutableGt;
import de.ii.xtraplatform.cql.domain.ImmutableGte;
import de.ii.xtraplatform.cql.domain.ImmutableIn;
import de.ii.xtraplatform.cql.domain.ImmutableIsNull;
import de.ii.xtraplatform.cql.domain.ImmutableLike;
import de.ii.xtraplatform.cql.domain.ImmutableLt;
import de.ii.xtraplatform.cql.domain.ImmutableLte;
import de.ii.xtraplatform.cql.domain.ImmutableNeq;
import de.ii.xtraplatform.cql.domain.In;
import de.ii.xtraplatform.cql.domain.IsNull;
import de.ii.xtraplatform.cql.domain.Like;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.Operand;
import de.ii.xtraplatform.cql.domain.Or;
import de.ii.xtraplatform.cql.domain.PositionNode;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.Scalar;
import de.ii.xtraplatform.cql.domain.ScalarLiteral;
import de.ii.xtraplatform.cql.domain.Spatial;
import de.ii.xtraplatform.cql.domain.SpatialFunction;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.cql.domain.Temporal;
import de.ii.xtraplatform.cql.domain.TemporalFunction;
import de.ii.xtraplatform.cql.domain.TemporalLiteral;
import de.ii.xtraplatform.cql.domain.Vector;
import de.ii.xtraplatform.cql.infra.CqlParser.GeometryCollectionContext;
import de.ii.xtraplatform.cql.infra.CqlParser.PatternExpressionContext;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.ImmutableGeometryCollection;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.MultiLineString;
import de.ii.xtraplatform.geometries.domain.MultiPoint;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.antlr.v4.runtime.tree.ParseTree;

public class CqlTextVisitor extends CqlParserBaseVisitor<CqlNode>
    implements CqlParserVisitor<CqlNode> {

  private final EpsgCrs defaultCrs;

  public CqlTextVisitor(EpsgCrs defaultCrs) {
    this.defaultCrs = defaultCrs;
  }

  @Override
  public CqlNode visitCqlFilter(CqlParser.CqlFilterContext ctx) {
    return ctx.booleanExpression().accept(this);
  }

  @Override
  public CqlNode visitNestedCqlFilter(CqlParser.NestedCqlFilterContext ctx) {
    return ctx.booleanExpression().accept(this);
  }

  @Override
  public CqlNode visitBooleanExpression(CqlParser.BooleanExpressionContext ctx) {
    int terms = ctx.booleanTerm().size();

    if (terms == 1) {
      return ctx.booleanTerm(0).accept(this);
    }

    return Or.of(
        IntStream.range(0, terms)
            .mapToObj(i -> (Cql2Expression) ctx.booleanTerm(i).accept(this))
            .collect(Collectors.toList()));
  }

  @Override
  public CqlNode visitBooleanTerm(CqlParser.BooleanTermContext ctx) {
    int factors = ctx.booleanFactor().size();

    if (factors == 1) {
      return ctx.booleanFactor(0).accept(this);
    }

    return And.of(
        IntStream.range(0, factors)
            .mapToObj(i -> (Cql2Expression) ctx.booleanFactor(i).accept(this))
            .collect(Collectors.toList()));
  }

  @Override
  public CqlNode visitBooleanFactor(CqlParser.BooleanFactorContext ctx) {
    CqlNode booleanPrimary = ctx.booleanPrimary().accept(this);
    if (Objects.nonNull(ctx.booleanPrimary().LEFTPAREN())) {
      booleanPrimary = ctx.booleanPrimary().booleanExpression().accept(this);
    }
    if (Objects.nonNull(ctx.NOT())) {
      return Not.of((Cql2Expression) booleanPrimary);
    }

    return booleanPrimary;
  }

  @Override
  public CqlNode visitBinaryComparisonPredicate(CqlParser.BinaryComparisonPredicateContext ctx) {

    ComparisonOperator comparisonOperator =
        ComparisonOperator.valueOfCqlText(ctx.ComparisonOperator().getText());

    Scalar scalar1 = (Scalar) ctx.scalarExpression(0).accept(this);
    Scalar scalar2 = (Scalar) ctx.scalarExpression(1).accept(this);

    BinaryScalarOperation.Builder<? extends BinaryScalarOperation> builder;

    switch (comparisonOperator) {
      case EQ:
        builder = new ImmutableEq.Builder();
        break;
      case NEQ:
        builder = new ImmutableNeq.Builder();
        break;
      case GT:
        builder = new ImmutableGt.Builder();
        break;
      case GTEQ:
        builder = new ImmutableGte.Builder();
        break;
      case LT:
        builder = new ImmutableLt.Builder();
        break;
      case LTEQ:
        builder = new ImmutableLte.Builder();
        break;
      default:
        throw new IllegalStateException("unknown comparison operator: " + comparisonOperator);
    }

    return builder.args(ImmutableList.of(scalar1, scalar2)).build();
  }

  @Override
  public CqlNode visitIsLikePredicate(CqlParser.IsLikePredicateContext ctx) {

    if (Objects.nonNull(ctx.LIKE())) {

      Scalar scalar1 = (Scalar) ctx.characterExpression().accept(this);
      Scalar scalar2 = (Scalar) ctx.patternExpression().accept(this);

      Like like = new ImmutableLike.Builder().args(ImmutableList.of(scalar1, scalar2)).build();

      if (Objects.nonNull(ctx.NOT())) {
        return Not.of(like);
      }

      return like;
    }
    return null;
  }

  @Override
  public CqlNode visitIsBetweenPredicate(CqlParser.IsBetweenPredicateContext ctx) {

    if (Objects.nonNull(ctx.BETWEEN())) {

      Scalar scalar = (Scalar) ctx.numericExpression(0).accept(this);

      Scalar numeric1 = (Scalar) ctx.numericExpression(1).accept(this);
      Scalar numeric2 = (Scalar) ctx.numericExpression(2).accept(this);

      Between between = new ImmutableBetween.Builder().addArgs(scalar, numeric1, numeric2).build();

      if (Objects.nonNull(ctx.NOT())) {
        return Not.of(between);
      }
      return between;
    }
    return null;
  }

  @Override
  public CqlNode visitIsInListPredicate(CqlParser.IsInListPredicateContext ctx) {
    In in;
    List<Scalar> values =
        ImmutableList.of(ctx.scalarExpression().subList(1, ctx.scalarExpression().size())).stream()
            .flatMap(Collection::stream)
            .map(v -> (Scalar) v.accept(this))
            .collect(Collectors.toList());

    // TODO IN currently requires a property on the left side and literals on the right side
    in =
        new ImmutableIn.Builder()
            .addArgs((Scalar) ctx.scalarExpression(0).accept(this))
            .addArgs(ArrayLiteral.of(values))
            .build();
    if (Objects.nonNull(ctx.NOT())) {
      return Not.of(in);
    }
    return in;
  }

  @Override
  public CqlNode visitIsNullPredicate(CqlParser.IsNullPredicateContext ctx) {

    if (Objects.nonNull(ctx.IS())) {
      Scalar scalar1 = (Scalar) ctx.isNullOperand().accept(this);

      IsNull isNull = new ImmutableIsNull.Builder().addArgs(scalar1).build();
      if (Objects.nonNull(ctx.NOT())) {
        return Not.of(isNull);
      }

      return isNull;
    }
    return null;
  }

  @Override
  public CqlNode visitTemporalPredicate(CqlParser.TemporalPredicateContext ctx) {

    if (Objects.isNull(ctx.TemporalFunction()))
      throw new IllegalStateException("unknown temporal predicate: " + ctx.getText());

    TemporalFunction temporalFunction =
        TemporalFunction.valueOf(ctx.TemporalFunction().getText().toUpperCase());

    Temporal temporal1 = (Temporal) ctx.temporalExpression(0).accept(this);
    Temporal temporal2 = (Temporal) ctx.temporalExpression(1).accept(this);

    return BinaryTemporalOperation.of(temporalFunction, temporal1, temporal2);
  }

  @Override
  public CqlNode visitSpatialPredicate(CqlParser.SpatialPredicateContext ctx) {

    if (Objects.isNull(ctx.SpatialFunction()))
      throw new IllegalStateException("unknown spatial operator: " + ctx.getText());

    SpatialFunction spatialFunction =
        SpatialFunction.valueOf(ctx.SpatialFunction().getText().toUpperCase());

    Spatial spatial1 = (Spatial) ctx.geomExpression().get(0).accept(this);
    Spatial spatial2 = (Spatial) ctx.geomExpression().get(1).accept(this);

    return BinarySpatialOperation.of(spatialFunction, spatial1, spatial2);
  }

  @Override
  public CqlNode visitArrayPredicate(CqlParser.ArrayPredicateContext ctx) {

    if (Objects.isNull(ctx.ArrayFunction()))
      throw new IllegalStateException("unknown array operator: " + ctx.getText());

    ArrayFunction arrayFunction =
        ArrayFunction.valueOf(ctx.ArrayFunction().getText().toUpperCase());

    Vector vector1 = (Vector) ctx.arrayExpression().get(0).accept(this);
    Vector vector2 = (Vector) ctx.arrayExpression().get(1).accept(this);

    return BinaryArrayOperation.of(arrayFunction, vector1, vector2);
  }

  @Override
  public CqlNode visitArrayClause(CqlParser.ArrayClauseContext ctx) {
    try {
      List<Scalar> values =
          ctx.arrayElement().stream()
              .map(e -> (ScalarLiteral) e.accept(this))
              .collect(Collectors.toList());
      return ArrayLiteral.of(values);
    } catch (CqlParseException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @Override
  public CqlNode visitPropertyName(CqlParser.PropertyNameContext ctx) {
    if (!ctx.nestedCqlFilter().isEmpty()) {
      Map<String, Cql2Expression> nestedFilters = new HashMap<>();
      for (int i = 0; i < ctx.nestedCqlFilter().size(); i++) {
        Cql2Expression nestedFilter = (Cql2Expression) ctx.nestedCqlFilter(i).accept(this);
        CqlVisitorPropertyPrefix prefix =
            new CqlVisitorPropertyPrefix(stripDoubleQuotes(ctx.Identifier(i).getText()));
        nestedFilter = (Cql2Expression) nestedFilter.accept(prefix);
        nestedFilters.put(stripDoubleQuotes(ctx.Identifier(i).getText()), nestedFilter);
      }
      String path =
          ctx.Identifier().stream()
              .map(ParseTree::getText)
              .map(this::stripDoubleQuotes)
              .collect(Collectors.joining("."));
      return Property.of(path, nestedFilters);
    }
    return Property.of(
        ctx.Identifier().stream()
            .map(ParseTree::getText)
            .map(this::stripDoubleQuotes)
            .collect(Collectors.joining(".")));
  }

  private String stripDoubleQuotes(String identifier) {
    if (identifier.startsWith("\"") && identifier.endsWith("\"")) {
      return identifier.substring(1, identifier.length() - 1);
    }
    return identifier;
  }

  @Override
  public CqlNode visitNumericLiteral(CqlParser.NumericLiteralContext ctx) {
    return ScalarLiteral.of(ctx.getText(), true);
  }

  @Override
  public CqlNode visitBooleanLiteral(CqlParser.BooleanLiteralContext ctx) {
    if (ctx.parent instanceof CqlParser.BooleanPrimaryContext) {
      return BooleanValue2.of(java.lang.Boolean.valueOf(ctx.getText()));
    }

    return ScalarLiteral.of(ctx.getText(), true);
  }

  @Override
  public CqlNode visitTemporalClause(CqlParser.TemporalClauseContext ctx) {
    try {
      if (Objects.nonNull(ctx.interval())) {
        return ctx.interval().accept(this);
      }
    } catch (CqlParseException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    return ctx.instantInstance().accept(this);
  }

  @Override
  public CqlNode visitInterval(CqlParser.IntervalContext ctx) {
    CqlNode arg1 = ctx.intervalParameter(0).accept(this);
    CqlNode arg2 = ctx.intervalParameter(1).accept(this);

    if (arg1 instanceof Temporal && arg2 instanceof Temporal) {
      return TemporalLiteral.interval((Temporal) arg1, (Temporal) arg2);
    }

    throw new IllegalStateException("unsupported interval value: " + ctx.getText());
  }

  @Override
  public CqlNode visitIntervalParameter(CqlParser.IntervalParameterContext ctx) {
    if (Objects.nonNull(ctx.NOW())) {
      return TemporalLiteral.of(Instant.now().truncatedTo(ChronoUnit.SECONDS));
    } else if (Objects.nonNull(ctx.DateString())) {
      String s = ctx.DateString().getText();
      return TemporalLiteral.of(s.substring(1, s.length() - 1));
    } else if (Objects.nonNull(ctx.TimestampString())) {
      String s = ctx.TimestampString().getText();
      return TemporalLiteral.of(s.substring(1, s.length() - 1));
    } else if (Objects.nonNull(ctx.DotDotString())) {
      return TemporalLiteral.of("..");
    } else if (Objects.nonNull(ctx.propertyName())) {
      return ctx.propertyName().accept(this);
    }
    throw new IllegalStateException("unsupported interval parameter: " + ctx.getText());
  }

  @Override
  public CqlNode visitInstantInstance(CqlParser.InstantInstanceContext ctx) {
    if (Objects.nonNull(ctx.NOW())) {
      return TemporalLiteral.of(Instant.now().truncatedTo(ChronoUnit.SECONDS));
    }

    String s =
        Objects.nonNull(ctx.DATE()) ? ctx.DateString().getText() : ctx.TimestampString().getText();
    return TemporalLiteral.of(s.substring(1, s.length() - 1));
  }

  @Override
  public CqlNode visitCharacterClause(CqlParser.CharacterClauseContext ctx) {
    if (Objects.nonNull(ctx.CASEI())) {
      Scalar scalar = (Scalar) ctx.characterExpression().accept(this);

      return Casei.of(scalar);
    } else if (Objects.nonNull(ctx.ACCENTI())) {
      Scalar scalar = (Scalar) ctx.characterExpression().accept(this);

      return Accenti.of(scalar);
    } else if (Objects.nonNull(ctx.LOWER())) {
      Scalar scalar = (Scalar) ctx.characterExpression().accept(this);

      return Function.of("LOWER", ImmutableList.of(scalar));
    } else if (Objects.nonNull(ctx.UPPER())) {
      Scalar scalar = (Scalar) ctx.characterExpression().accept(this);

      return Function.of("UPPER", ImmutableList.of(scalar));
    }

    return ctx.characterLiteral().accept(this);
  }

  @Override
  public CqlNode visitPatternExpression(PatternExpressionContext ctx) {
    if (Objects.nonNull(ctx.CASEI())) {
      Scalar scalar = (Scalar) ctx.patternExpression().accept(this);

      return Casei.of(scalar);
    } else if (Objects.nonNull(ctx.ACCENTI())) {
      Scalar scalar = (Scalar) ctx.patternExpression().accept(this);

      return Accenti.of(scalar);
    } else if (Objects.nonNull(ctx.LOWER())) {
      Scalar scalar = (Scalar) ctx.patternExpression().accept(this);

      return Function.of("LOWER", ImmutableList.of(scalar));
    } else if (Objects.nonNull(ctx.UPPER())) {
      Scalar scalar = (Scalar) ctx.patternExpression().accept(this);

      return Function.of("UPPER", ImmutableList.of(scalar));
    }

    return ctx.characterLiteral().accept(this);
  }

  @Override
  public CqlNode visitCharacterLiteral(CqlParser.CharacterLiteralContext ctx) {
    return getScalarLiteralFromText(ctx.getText());
  }

  @Override
  public CqlNode visitGeometryLiteral(CqlParser.GeometryLiteralContext ctx) {
    CqlNode geomLiteral = ctx.getChild(0).accept(this);
    if (geomLiteral instanceof PositionNode posNode) {
      return SpatialLiteral.of(Point.of(posNode.getPosition(), Optional.of(defaultCrs)));
    }
    return SpatialLiteral.of(((GeometryNode) geomLiteral).getGeometry());
  }

  @Override
  public CqlNode visitPoint(CqlParser.PointContext ctx) {
    return ctx.coordinate().accept(this);
  }

  @Override
  public CqlNode visitMultiPoint(CqlParser.MultiPointContext ctx) {
    List<Point> points =
        ctx.coordinate().stream()
            .map(coordinateContext -> coordinateContext.accept(this))
            .map(point -> Point.of(((PositionNode) point).getPosition(), Optional.of(defaultCrs)))
            .toList();

    return GeometryNode.of(MultiPoint.of(points, Optional.of(defaultCrs)));
  }

  @Override
  public CqlNode visitMultiLinestring(CqlParser.MultiLinestringContext ctx) {
    List<LineString> lineStrings =
        ctx.linestringDef().stream()
            .map(
                linestringContext ->
                    (LineString) ((GeometryNode) linestringContext.accept(this)).getGeometry())
            .collect(Collectors.toList());

    return GeometryNode.of(MultiLineString.of(lineStrings, Optional.of(defaultCrs)));
  }

  @Override
  public CqlNode visitMultiPolygon(CqlParser.MultiPolygonContext ctx) {
    List<Polygon> polygons =
        ctx.polygonDef().stream()
            .map(
                polygonDefContext ->
                    (Polygon) ((GeometryNode) (polygonDefContext.accept(this))).getGeometry())
            .toList();

    return GeometryNode.of(MultiPolygon.of(polygons, Optional.of(defaultCrs)));
  }

  @Override
  public CqlNode visitPolygonDef(CqlParser.PolygonDefContext ctx) {
    List<PositionList> rings =
        ctx.linestringDef().stream()
            .map(
                linestringContext ->
                    (PositionList)
                        ((GeometryNode) linestringContext.accept(this)).getGeometry().getValue())
            .toList();

    return GeometryNode.of(Polygon.of(rings, Optional.of(defaultCrs)));
  }

  @Override
  public CqlNode visitLinestringDef(CqlParser.LinestringDefContext ctx) {
    List<Position> coordinates =
        ctx.coordinate().stream()
            .map(pointContext -> ((PositionNode) pointContext.accept(this)).getPosition())
            .toList();

    return GeometryNode.of(LineString.of(PositionList.of(coordinates), Optional.of(defaultCrs)));
  }

  @Override
  public CqlNode visitBbox(CqlParser.BboxContext ctx) {
    List<Double> coordinates;
    Double eastBoundLon = Double.valueOf(ctx.eastBoundLon().NumericLiteral().getText());
    Double westBoundLon = Double.valueOf(ctx.westBoundLon().NumericLiteral().getText());
    Double northBoundLat = Double.valueOf(ctx.northBoundLat().NumericLiteral().getText());
    Double southBoundLat = Double.valueOf(ctx.southBoundLat().NumericLiteral().getText());

    if (Objects.nonNull(ctx.minElev()) && Objects.nonNull(ctx.maxElev())) {
      Double minElev = Double.valueOf(ctx.minElev().NumericLiteral().getText());
      Double maxElev = Double.valueOf(ctx.maxElev().NumericLiteral().getText());
      coordinates =
          ImmutableList.of(
              southBoundLat, westBoundLon, minElev, northBoundLat, eastBoundLon, maxElev);
    } else {
      coordinates = ImmutableList.of(westBoundLon, southBoundLat, eastBoundLon, northBoundLat);
    }

    return SpatialLiteral.of(
        new ImmutableBbox.Builder().coordinates(coordinates).crs(defaultCrs).build());
  }

  @Override
  public CqlNode visitGeometryCollection(GeometryCollectionContext ctx) {
    ImmutableGeometryCollection.Builder builder = ImmutableGeometryCollection.builder();
    IntStream.range(0, ctx.geometryLiteral().size())
        .forEach(
            i ->
                builder.addValue(
                    ((GeometryNode)
                            ((SpatialLiteral) ctx.geometryLiteral(i).accept(this)).getValue())
                        .getGeometry()));

    return SpatialLiteral.of(builder.crs(defaultCrs).build());
  }

  @Override
  public CqlNode visitCoordinate(CqlParser.CoordinateContext ctx) {
    double x = Double.parseDouble(ctx.xCoord().getText());
    double y = Double.parseDouble(ctx.yCoord().getText());

    if (Objects.nonNull(ctx.zCoord())) {
      double z = Double.parseDouble(ctx.zCoord().getText());
      return PositionNode.of(Position.ofXYZ(x, y, z));
    }

    return PositionNode.of(Position.ofXY(x, y));
  }

  @Override
  public CqlNode visitFunction(CqlParser.FunctionContext ctx) {
    String functionName = ctx.Identifier().getText();
    if (Objects.isNull(ctx.argumentList().positionalArgument())) {
      return Function.of(functionName, ImmutableList.of());
    }

    List<Operand> args =
        ctx.argumentList().positionalArgument().argument().stream()
            .map(arg -> (Operand) arg.accept(this))
            .collect(Collectors.toList());
    return Function.of(functionName, args);
  }

  private ScalarLiteral getScalarLiteralFromText(String cqlText) {
    return ScalarLiteral.of(cqlText.substring(1, cqlText.length() - 1).replaceAll("''", "'"));
  }
}
