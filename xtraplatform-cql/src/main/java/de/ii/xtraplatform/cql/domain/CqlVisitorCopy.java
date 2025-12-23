/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CqlVisitorCopy implements CqlVisitor<CqlNode> {

  @Override
  public CqlNode visit(LogicalOperation logicalOperation, List<CqlNode> children) {
    if (logicalOperation instanceof And) {
      return And.of(
          children.stream().map(cqlNode -> (Cql2Expression) cqlNode).collect(Collectors.toList()));
    } else if (logicalOperation instanceof Or) {
      return Or.of(
          children.stream().map(cqlNode -> (Cql2Expression) cqlNode).collect(Collectors.toList()));
    }
    return null;
  }

  @Override
  public CqlNode visit(Not not, List<CqlNode> children) {
    return Not.of((Cql2Expression) children.get(0));
  }

  @Override
  public CqlNode visit(BinaryScalarOperation scalarOperation, List<CqlNode> children) {
    BinaryScalarOperation.Builder<?> builder = null;

    if (scalarOperation instanceof Eq) {
      builder = new ImmutableEq.Builder();
    } else if (scalarOperation instanceof Gt) {
      builder = new ImmutableGt.Builder();
    } else if (scalarOperation instanceof Gte) {
      builder = new ImmutableGte.Builder();
    } else if (scalarOperation instanceof Lt) {
      builder = new ImmutableLt.Builder();
    } else if (scalarOperation instanceof Lte) {
      builder = new ImmutableLte.Builder();
    } else if (scalarOperation instanceof Neq) {
      builder = new ImmutableNeq.Builder();
    }

    if (Objects.nonNull(builder)) {
      return builder
          .args(
              children.stream()
                  .filter(child -> child instanceof Scalar)
                  .map(child -> (Scalar) child)
                  .toList())
          .build();
    }

    return null;
  }

  @Override
  public CqlNode visit(Between between, List<CqlNode> children) {
    Between.Builder builder = new ImmutableBetween.Builder();

    int i = 0;
    for (CqlNode cqlNode : children) {
      switch (i++) {
        case 0:
        case 1:
        case 2:
          builder.addArgs((Scalar) cqlNode);
          break;
      }
    }
    return builder.build();
  }

  @Override
  public CqlNode visit(IsNull isNull, List<CqlNode> children) {
    IsNull.Builder builder = new ImmutableIsNull.Builder();
    builder.addArgs((Scalar) children.get(0));
    return builder.build();
  }

  @Override
  public CqlNode visit(Casei casei, List<CqlNode> children) {
    ImmutableCasei.Builder builder = new ImmutableCasei.Builder();
    builder.value((Scalar) children.get(0));
    return builder.build();
  }

  @Override
  public CqlNode visit(Accenti accenti, List<CqlNode> children) {
    ImmutableAccenti.Builder builder = new ImmutableAccenti.Builder();
    builder.value((Scalar) children.get(0));
    return builder.build();
  }

  @Override
  public CqlNode visit(Interval interval, List<CqlNode> children) {
    ImmutableInterval.Builder builder = new ImmutableInterval.Builder();
    return builder.args(children.stream().map(child -> (Operand) child).toList()).build();
  }

  @Override
  public CqlNode visit(Like like, List<CqlNode> children) {
    Like.Builder builder = new ImmutableLike.Builder();

    // modifiers are set separately
    return builder
        .args(
            children.stream()
                .filter(child -> child instanceof Scalar)
                .map(child -> (Scalar) child)
                .toList())
        .build();
  }

  @Override
  public CqlNode visit(In in, List<CqlNode> children) {
    ImmutableIn.Builder builder = new ImmutableIn.Builder();

    builder.addArgs((Scalar) children.get(0));
    ArrayList<Scalar> list = new ArrayList<>();
    for (int i = 1; i < children.size(); i++) {
      list.add((Scalar) children.get(i));
    }
    builder.addAllArgs(list);
    return builder.build();
  }

  @Override
  public CqlNode visit(BinaryTemporalOperation temporalOperation, List<CqlNode> children) {
    List<Temporal> temporals =
        children.stream()
            .filter(child -> child instanceof Temporal)
            .map(child -> (Temporal) child)
            .toList();
    return BinaryTemporalOperation.of(
        temporalOperation.getTemporalOperator(), temporals.get(0), temporals.get(1));
  }

  @Override
  public CqlNode visit(BinarySpatialOperation spatialOperation, List<CqlNode> children) {
    List<Spatial> spatials =
        children.stream()
            .filter(child -> child instanceof Spatial)
            .map(child -> (Spatial) child)
            .toList();
    return BinarySpatialOperation.of(
        spatialOperation.getSpatialOperator(), spatials.get(0), spatials.get(1));
  }

  @Override
  public CqlNode visit(BinaryArrayOperation arrayOperation, List<CqlNode> children) {
    List<Vector> vectors =
        children.stream()
            .filter(child -> child instanceof Vector)
            .map(child -> (Vector) child)
            .toList();
    return BinaryArrayOperation.of(
        arrayOperation.getArrayOperator(), vectors.get(0), vectors.get(1));
  }

  @Override
  public CqlNode visit(ScalarLiteral scalarLiteral, List<CqlNode> children) {
    return scalarLiteral;
  }

  @Override
  public CqlNode visit(TemporalLiteral temporalLiteral, List<CqlNode> children) {
    return temporalLiteral;
  }

  @Override
  public CqlNode visit(ArrayLiteral arrayLiteral, List<CqlNode> children) {
    return arrayLiteral;
  }

  @Override
  public CqlNode visit(SpatialLiteral spatialLiteral, List<CqlNode> children) {
    return spatialLiteral;
  }

  @Override
  public CqlNode visit(Property property, List<CqlNode> children) {
    return property;
  }

  @Override
  public CqlNode visit(PositionNode position, List<CqlNode> children) {
    return position;
  }

  @Override
  public CqlNode visit(GeometryNode geometry, List<CqlNode> children) {
    return geometry;
  }

  @Override
  public CqlNode visit(Bbox bbox, List<CqlNode> children) {
    return bbox;
  }

  @Override
  public CqlNode visit(Function function, List<CqlNode> children) {
    return function;
  }

  @Override
  public CqlNode visit(BooleanValue2 booleanValue, List<CqlNode> children) {
    return booleanValue;
  }

  @Override
  public CqlNode visit(Parameter parameter, List<CqlNode> children) {
    return parameter;
  }
}
