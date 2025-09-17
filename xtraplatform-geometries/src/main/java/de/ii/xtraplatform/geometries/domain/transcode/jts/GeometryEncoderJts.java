/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transcode.jts;

import static de.ii.xtraplatform.base.domain.util.LambdaWithException.mayThrow;

import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.CompoundCurve;
import de.ii.xtraplatform.geometries.domain.CurvePolygon;
import de.ii.xtraplatform.geometries.domain.GeometryCollection;
import de.ii.xtraplatform.geometries.domain.MultiCurve;
import de.ii.xtraplatform.geometries.domain.MultiLineString;
import de.ii.xtraplatform.geometries.domain.MultiPoint;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.MultiSurface;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.PolyhedralSurface;
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import de.ii.xtraplatform.geometries.domain.transform.GeometryVisitor;
import de.ii.xtraplatform.geometries.domain.transform.ToSimpleFeatures;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFactory;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;

public class GeometryEncoderJts implements GeometryVisitor<org.locationtech.jts.geom.Geometry> {

  private final GeometryFactory geometryFactory;
  private final ToSimpleFeatures toSimpleFeatures;

  public GeometryEncoderJts(GeometryFactory geometryFactory) {
    this.geometryFactory = geometryFactory;
    this.toSimpleFeatures = new ToSimpleFeatures();
  }

  private Coordinate getJtsCoordinate(Position position) {
    Coordinate coordinate = new Coordinate();
    coordinate.setX(position.getCoordinates()[0]);
    coordinate.setY(position.getCoordinates()[1]);
    if (position.getAxes() == Axes.XYZ || position.getAxes() == Axes.XYZM) {
      coordinate.setZ(position.getCoordinates()[2]);
    }
    return coordinate;
  }

  private CoordinateSequence getJtsCoordinateSequence(PositionList positionList) {
    CoordinateSequenceFactory factory = geometryFactory.getCoordinateSequenceFactory();
    double[] coordinates = positionList.getCoordinates();
    int dim = (positionList.getAxes() == Axes.XYZ || positionList.getAxes() == Axes.XYZM) ? 3 : 2;
    CoordinateSequence sequence = factory.create(positionList.getNumPositions(), dim);
    for (int i = 0; i < positionList.getNumPositions(); i++) {
      sequence.setOrdinate(i, 0, coordinates[positionList.getAxes().size() * i]);
      sequence.setOrdinate(i, 1, coordinates[positionList.getAxes().size() * i + 1]);
      if (dim == 3) {
        sequence.setOrdinate(i, 2, coordinates[positionList.getAxes().size() * i + 2]);
      }
    }
    return sequence;
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(Point geometry) {
    if (geometry.isEmpty()) {
      return geometryFactory.createPoint();
    }
    return geometryFactory.createPoint(getJtsCoordinate(geometry.getValue()));
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(MultiPoint geometry) {
    return geometryFactory.createMultiPoint(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .filter(org.locationtech.jts.geom.Point.class::isInstance)
            .map(org.locationtech.jts.geom.Point.class::cast)
            .toArray(org.locationtech.jts.geom.Point[]::new));
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(SingleCurve geometry) {
    if (geometry.isEmpty()) {
      return geometryFactory.createLineString();
    }
    return geometryFactory.createLineString(getJtsCoordinateSequence(geometry.getValue()));
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(MultiLineString geometry) {
    return geometryFactory.createMultiLineString(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .filter(org.locationtech.jts.geom.LineString.class::isInstance)
            .map(org.locationtech.jts.geom.LineString.class::cast)
            .toArray(org.locationtech.jts.geom.LineString[]::new));
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(Polygon geometry) {
    if (geometry.isEmpty()) {
      return geometryFactory.createPolygon();
    }
    LinearRing outerRing =
        geometryFactory.createLinearRing(
            getJtsCoordinateSequence(geometry.getValue().get(0).getValue()));
    LinearRing[] innerRings =
        geometry.getValue().subList(1, geometry.getNumRings()).stream()
            .map(
                ring -> geometryFactory.createLinearRing(getJtsCoordinateSequence(ring.getValue())))
            .toArray(LinearRing[]::new);
    return geometryFactory.createPolygon(outerRing, innerRings);
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(MultiPolygon geometry) {
    return geometryFactory.createMultiPolygon(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .filter(org.locationtech.jts.geom.Polygon.class::isInstance)
            .map(org.locationtech.jts.geom.Polygon.class::cast)
            .toArray(org.locationtech.jts.geom.Polygon[]::new));
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(GeometryCollection geometry) {
    return geometryFactory.createGeometryCollection(
        geometry.getValue().stream()
            .map(mayThrow(geom -> geom.accept(this)))
            .toArray(org.locationtech.jts.geom.Geometry[]::new));
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(CompoundCurve geometry) {
    return geometry.accept(toSimpleFeatures).accept(this);
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(CurvePolygon geometry) {
    return geometry.accept(toSimpleFeatures).accept(this);
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(MultiCurve geometry) {
    return geometry.accept(toSimpleFeatures).accept(this);
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(MultiSurface geometry) {
    return geometry.accept(toSimpleFeatures).accept(this);
  }

  @Override
  public org.locationtech.jts.geom.Geometry visit(PolyhedralSurface geometry) {
    return geometry.accept(toSimpleFeatures).accept(this);
  }
}
