/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Modifiable;

public interface GeometryWithStringCoordinates<T>
    extends Geometry<T>, Geometry.ModifiableGeometry<String> {

  static GeometryWithStringCoordinates<?> of(SimpleFeatureGeometry type, OptionalInt dimension) {
    return (GeometryWithStringCoordinates<?>)
        switch (type) {
          case POINT -> ModifiablePoint.create().setDimension(dimension);
          case LINE_STRING -> ModifiableLineString.create().setDimension(dimension);
          case POLYGON -> ModifiablePolygon.create().setDimension(dimension);
          case MULTI_POINT -> ModifiableMultiPoint.create().setDimension(dimension);
          case MULTI_LINE_STRING -> ModifiableMultiLineString.create().setDimension(dimension);
          case MULTI_POLYGON -> ModifiableMultiPolygon.create().setDimension(dimension);
          case GEOMETRY_COLLECTION -> throw new IllegalArgumentException(
              "GeometryCollection not supported");
          case ANY -> throw new IllegalArgumentException("Any not supported");
          case NONE -> throw new IllegalArgumentException("None not supported");
        };
  }

  @Modifiable
  interface Point extends GeometryWithStringCoordinates<String> {

    @Check
    default void check() {
      Preconditions.checkState(
          getCoordinates().size() == getDimension(),
          "a point must have only one coordinate",
          getCoordinates().size());
    }

    @Derived
    @Override
    default SimpleFeatureGeometry getType() {
      return SimpleFeatureGeometry.POINT;
    }

    @Override
    default void openChild() {}

    @Override
    default void closeChild() {}

    @Override
    default void addCoordinate(String coordinate) {
      getCoordinates().add(coordinate);
    }
  }

  @Modifiable
  interface LineString extends GeometryWithStringCoordinates<String> {

    @Derived
    @Override
    default SimpleFeatureGeometry getType() {
      return SimpleFeatureGeometry.LINE_STRING;
    }

    @Override
    default void openChild() {}

    @Override
    default void closeChild() {}

    @Override
    default void addCoordinate(String coordinate) {
      getCoordinates().add(coordinate);
    }
  }

  @Modifiable
  interface Polygon extends GeometryWithStringCoordinates<List<String>> {

    @Derived
    @Override
    default SimpleFeatureGeometry getType() {
      return SimpleFeatureGeometry.POLYGON;
    }

    @Override
    default void openChild() {
      if (getDepth() == 0) {
        setDepth(1);
      } else {
        getCoordinates().add(new ArrayList<>());
      }
    }

    @Override
    default void closeChild() {}

    @Override
    default void addCoordinate(String coordinate) {
      if (getCoordinates().isEmpty()) {
        throw new IllegalStateException("No child polygon opened");
      }
      List<String> lastRing = getCoordinates().get(getCoordinates().size() - 1);
      if (lastRing != null) {
        lastRing.add(coordinate);
      } else {
        throw new IllegalStateException("No child polygon opened");
      }
    }
  }

  @Modifiable
  interface MultiPoint extends GeometryWithStringCoordinates<List<String>> {

    @Derived
    @Override
    default SimpleFeatureGeometry getType() {
      return SimpleFeatureGeometry.MULTI_POINT;
    }

    @Override
    default void openChild() {
      getCoordinates().add(new ArrayList<>());
    }

    @Override
    default void closeChild() {}

    @Override
    default void addCoordinate(String coordinate) {
      if (getCoordinates().isEmpty()) {
        throw new IllegalStateException("No child point opened");
      }
      List<String> lastPoint = getCoordinates().get(getCoordinates().size() - 1);
      if (lastPoint != null) {
        lastPoint.add(coordinate);
      } else {
        throw new IllegalStateException("No child point opened");
      }
    }
  }

  @Modifiable
  interface MultiLineString extends GeometryWithStringCoordinates<List<String>> {

    @Derived
    @Override
    default SimpleFeatureGeometry getType() {
      return SimpleFeatureGeometry.MULTI_LINE_STRING;
    }

    @Override
    default void openChild() {
      if (getDepth() == 0) {
        setDepth(1);
      } else {
        getCoordinates().add(new ArrayList<>());
      }
    }

    @Override
    default void closeChild() {}

    @Override
    default void addCoordinate(String coordinate) {
      if (getCoordinates().isEmpty()) {
        throw new IllegalStateException("No child polygon opened");
      }
      List<String> lastLine = getCoordinates().get(getCoordinates().size() - 1);
      if (lastLine != null) {
        lastLine.add(coordinate);
      } else {
        throw new IllegalStateException("No child polygon opened");
      }
    }
  }

  @Modifiable
  interface MultiPolygon extends GeometryWithStringCoordinates<Polygon> {

    @Derived
    @Override
    default SimpleFeatureGeometry getType() {
      return SimpleFeatureGeometry.MULTI_POLYGON;
    }

    @Override
    default void openChild() {
      if (getDepth() == 1) {
        getCoordinates().add(ModifiablePolygon.create().setDimension(getDimension()).setDepth(1));
      } else if (getDepth() > 1) {
        getCoordinates().get(getCoordinates().size() - 1).openChild();
      }
      setDepth(getDepth() + 1);
    }

    @Override
    default void closeChild() {
      if (getDepth() > 1) {
        getCoordinates().get(getCoordinates().size() - 1).closeChild();
      }
      setDepth(getDepth() - 1);
    }

    @Override
    default void addCoordinate(String coordinate) {
      if (getCoordinates().isEmpty()) {
        throw new IllegalStateException("No child polygon opened");
      }
      Polygon lastPolygon = getCoordinates().get(getCoordinates().size() - 1);
      if (lastPolygon != null) {
        lastPolygon.addCoordinate(coordinate);
      } else {
        throw new IllegalStateException("No child polygon opened");
      }
    }
  }
}
