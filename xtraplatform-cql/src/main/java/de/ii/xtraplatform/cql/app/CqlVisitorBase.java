/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

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
import de.ii.xtraplatform.cql.domain.CqlVisitor;
import de.ii.xtraplatform.cql.domain.Function;
import de.ii.xtraplatform.cql.domain.GeometryNode;
import de.ii.xtraplatform.cql.domain.In;
import de.ii.xtraplatform.cql.domain.Interval;
import de.ii.xtraplatform.cql.domain.IsNull;
import de.ii.xtraplatform.cql.domain.Like;
import de.ii.xtraplatform.cql.domain.LogicalOperation;
import de.ii.xtraplatform.cql.domain.Not;
import de.ii.xtraplatform.cql.domain.PositionNode;
import de.ii.xtraplatform.cql.domain.Property;
import de.ii.xtraplatform.cql.domain.ScalarLiteral;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.cql.domain.TemporalLiteral;
import java.util.List;

public class CqlVisitorBase<T> implements CqlVisitor<T> {
  @Override
  public T visit(LogicalOperation logicalOperation, List<T> children) {
    return null;
  }

  @Override
  public T visit(Not not, List<T> children) {

    return null;
  }

  @Override
  public T visit(BinaryScalarOperation scalarOperation, List<T> children) {
    return null;
  }

  @Override
  public T visit(Between between, List<T> children) {
    return null;
  }

  @Override
  public T visit(Like like, List<T> children) {
    return null;
  }

  @Override
  public T visit(In in, List<T> children) {
    return null;
  }

  @Override
  public T visit(IsNull isNull, List<T> children) {
    return null;
  }

  @Override
  public T visit(Casei casei, List<T> children) {
    return null;
  }

  @Override
  public T visit(Accenti accenti, List<T> children) {
    return null;
  }

  @Override
  public T visit(Interval interval, List<T> children) {
    return null;
  }

  @Override
  public T visit(BinaryTemporalOperation temporalOperation, List<T> children) {
    return null;
  }

  @Override
  public T visit(BinarySpatialOperation spatialOperation, List<T> children) {
    return null;
  }

  @Override
  public T visit(BinaryArrayOperation arrayOperation, List<T> children) {
    return null;
  }

  @Override
  public T visit(ScalarLiteral scalarLiteral, List<T> children) {
    return null;
  }

  @Override
  public T visit(TemporalLiteral temporalLiteral, List<T> children) {
    return null;
  }

  @Override
  public T visit(ArrayLiteral arrayLiteral, List<T> children) {
    return null;
  }

  @Override
  public T visit(SpatialLiteral spatialLiteral, List<T> children) {
    return null;
  }

  @Override
  public T visit(Property property, List<T> children) {
    return null;
  }

  @Override
  public T visit(PositionNode position, List<T> children) {
    return null;
  }

  @Override
  public T visit(GeometryNode geometry, List<T> children) {
    return null;
  }

  @Override
  public T visit(Bbox bbox, List<T> children) {
    return null;
  }

  @Override
  public T visit(Function function, List<T> children) {
    return null;
  }

  @Override
  public T visit(BooleanValue2 booleanValue, List<T> children) {
    return null;
  }
}
