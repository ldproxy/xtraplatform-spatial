/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.cql.domain.Bbox;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.GeometryNode;
import de.ii.xtraplatform.cql.domain.PositionNode;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.CoordinateTuple;
import de.ii.xtraplatform.crs.domain.CrsInfo;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryCollection;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.MultiLineString;
import de.ii.xtraplatform.geometries.domain.MultiPoint;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CqlCoordinateChecker extends CqlVisitorBase<Object> {

  private static final List<String> AXES = ImmutableList.of("first", "second", "third");

  private final CrsInfo crsInfo;
  private final EpsgCrs filterCrs;
  private final Optional<EpsgCrs> nativeCrs;
  private final Optional<CrsTransformer> crsTransformerFilterToNative;
  private final Optional<CrsTransformer> crsTransformerFilterToCrs84;
  private final Optional<BoundingBox> domainOfValidityNative;
  private final Optional<BoundingBox> domainOfValidityFilter;

  public CqlCoordinateChecker(
      CrsTransformerFactory crsTransformerFactory,
      CrsInfo crsInfo,
      EpsgCrs filterCrs,
      EpsgCrs nativeCrs) {
    this.crsInfo = crsInfo;
    this.filterCrs = filterCrs;
    this.nativeCrs = Optional.ofNullable(nativeCrs);
    this.crsTransformerFilterToNative =
        this.nativeCrs.isPresent()
            ? crsTransformerFactory.getTransformer(filterCrs, nativeCrs, true)
            : Optional.empty();
    this.crsTransformerFilterToCrs84 =
        crsTransformerFactory.getTransformer(filterCrs, OgcCrs.CRS84, true);
    this.domainOfValidityFilter = crsInfo.getDomainOfValidity(filterCrs);
    this.domainOfValidityNative =
        this.nativeCrs.isPresent() ? crsInfo.getDomainOfValidity(nativeCrs) : Optional.empty();
  }

  @Override
  public Object visit(SpatialLiteral spatialLiteral, List<Object> children) {
    return ((CqlNode) spatialLiteral.getValue()).accept(this);
  }

  @Override
  public Object visit(GeometryNode geometry2, List<Object> children) {
    Geometry<?> geom = geometry2.getGeometry();
    switch (geom.getType()) {
      case POINT:
        checkPosition(((Point) geom).getValue());
        return null;
      case LINE_STRING:
        PositionList posList = ((LineString) geom).getValue();
        IntStream.range(0, posList.getNumPositions()).forEach(i -> checkPosition(posList.get(i)));
        return null;
      case POLYGON:
        ((Polygon) geom).getValue().forEach(ring -> GeometryNode.of(ring).accept(this));
        return null;
      case MULTI_POINT:
        ((MultiPoint) geom).getValue().forEach(point -> GeometryNode.of(point).accept(this));
        return null;
      case MULTI_LINE_STRING:
        ((MultiLineString) geom)
            .getValue()
            .forEach(lineString -> GeometryNode.of(lineString).accept(this));
        return null;
      case MULTI_POLYGON:
        ((MultiPolygon) geom).getValue().forEach(polygon -> GeometryNode.of(polygon).accept(this));
        return null;
      case GEOMETRY_COLLECTION:
        ((GeometryCollection) geom)
            .getValue()
            .forEach(geometry -> GeometryNode.of(geometry).accept(this));
        return null;
    }
    return null;
  }

  @Override
  public Object visit(Bbox bbox, List<Object> children) {
    List<Double> doubles = bbox.getCoordinates();
    Position ll = Position.ofXY(doubles.get(0), doubles.get(1));
    Position ur = Position.ofXY(doubles.get(2), doubles.get(3));
    visit(PositionNode.of(ll), ImmutableList.of());
    visit(PositionNode.of(ur), ImmutableList.of());

    int axisWithWraparound = crsInfo.getAxisWithWraparound(filterCrs).orElse(-1);
    IntStream.range(0, 2)
        .forEach(
            axis -> {
              if (axisWithWraparound != axis
                  && ll.getCoordinates()[axis] > ur.getCoordinates()[axis])
                throw new IllegalArgumentException(
                    String.format(
                        "The coordinates of the bounding box [[ %s ], [ %s ]] do not form a valid bounding box for coordinate reference system '%s'. The first value is larger than the second value for the %s axis.",
                        getCoordinatesAsString(ll),
                        getCoordinatesAsString(ur),
                        getCrsText(filterCrs),
                        AXES.get(axis)));
            });

    return null;
  }

  @Override
  public Object visit(PositionNode positionNode, List<Object> children) {
    checkPosition(positionNode.getPosition());
    return null;
  }

  private void checkPosition(Position pos) {
    checkPosition(pos, filterCrs);

    crsTransformerFilterToNative.ifPresent(
        t -> {
          CoordinateTuple transformed = t.transform(pos.x(), pos.y());
          if (Objects.isNull(transformed))
            throw new IllegalArgumentException(
                String.format(
                    "Filter is invalid. Coordinate '%s' cannot be transformed to %s.",
                    getCoordinatesAsString(pos), getCrsText(nativeCrs.get())));
          checkPosition(Position.ofXY(transformed.getX(), transformed.getY()), nativeCrs.get());
        });

    Position posCrs84 = pos;
    if (crsTransformerFilterToCrs84.isPresent()) {
      CoordinateTuple transformed = crsTransformerFilterToCrs84.get().transform(pos.x(), pos.y());
      if (Objects.nonNull(transformed)) {
        posCrs84 = Position.ofXY(transformed.getX(), transformed.getY());
      }
    }
    checkDomainOfValidity(posCrs84, domainOfValidityFilter, getCrsText(filterCrs));
    if (nativeCrs.isPresent()) {
      checkDomainOfValidity(posCrs84, domainOfValidityNative, getCrsText(nativeCrs.get()));
    }
  }

  private void checkPosition(Position position, EpsgCrs crs) {
    // check each axis against the constraints specified in the CRS definition
    List<Optional<Double>> minimums = crsInfo.getAxisMinimums(crs);
    List<Optional<Double>> maximums = crsInfo.getAxisMaximums(crs);
    IntStream.range(0, position.getAxes() == Axes.XY | position.getAxes() == Axes.XYM ? 2 : 3)
        .filter(i -> !Double.isNaN(position.getCoordinates()[i]))
        .filter(i -> minimums.get(i).isPresent() || maximums.get(i).isPresent())
        .forEach(
            i -> {
              minimums
                  .get(i)
                  .ifPresent(
                      min -> {
                        if (position.getCoordinates()[i] < min)
                          throw new IllegalArgumentException(
                              String.format(
                                  "The coordinate '%s' in the filter expression is invalid for %s. The value of the %s axis is smaller than the minimum value for the axis: %f.",
                                  getCoordinatesAsString(position),
                                  getCrsText(crs),
                                  AXES.get(i),
                                  min));
                      });
              maximums
                  .get(i)
                  .ifPresent(
                      max -> {
                        if (position.getCoordinates()[i] > max)
                          throw new IllegalArgumentException(
                              String.format(
                                  "The coordinate '%s' in the filter expression is invalid for %s. The value of the %s axis is larger than the maximum value for the axis: %f.",
                                  getCoordinatesAsString(position),
                                  getCrsText(crs),
                                  AXES.get(i),
                                  max));
                      });
            });
  }

  private void checkDomainOfValidity(
      Position position, Optional<BoundingBox> domainOfValidity, String crsText) {
    // validate against the domain of validity of the CRS
    domainOfValidity.ifPresent(
        bboxCrs84 -> {
          if (position.x() < bboxCrs84.getXmin()
              || position.x() > bboxCrs84.getXmax()
              || position.y() < bboxCrs84.getYmin()
              || position.y() > bboxCrs84.getYmax()) {
            throw new IllegalArgumentException(
                String.format(
                    "A coordinate in the filter expression is outside the domain of validity of %s. The coordinate converted to WGS 84 longitude/latitude is [ %s ], the domain of validity is [ %s ].",
                    crsText, getCoordinatesAsString(position), getCoordinatesAsString(bboxCrs84)));
          }
        });
  }

  private String getCrsText(EpsgCrs crs) {
    if (crs.equals(filterCrs))
      return String.format(
          "the coordinate reference system '%s' used in the query", crs.toHumanReadableString());
    else if (nativeCrs.isPresent() && crs.equals(nativeCrs.get()))
      return String.format(
          "the native coordinate reference system '%s' of the data", crs.toHumanReadableString());
    return String.format("the coordinate reference system '%s'", crs.toHumanReadableString());
  }

  private String getCoordinatesAsString(Position position) {
    return Arrays.stream(position.getCoordinates())
        .mapToObj(c -> String.format(Locale.US, "%.2f", c))
        .collect(Collectors.joining(", "));
  }

  private String getCoordinatesAsString(BoundingBox bbox) {
    return Arrays.stream(bbox.toArray())
        .mapToObj(c -> String.format(Locale.US, "%.2f", c))
        .collect(Collectors.joining(", "));
  }
}
