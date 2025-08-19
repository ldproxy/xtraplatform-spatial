/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.app;

import de.ii.xtraplatform.cql.domain.Bbox;
import de.ii.xtraplatform.cql.domain.CqlNode;
import de.ii.xtraplatform.cql.domain.GeometryNode;
import de.ii.xtraplatform.cql.domain.SpatialLiteral;
import de.ii.xtraplatform.crs.domain.CrsInfo;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.PositionList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CqlVisitorMapEnvelopes extends CqlVisitorCopy {

  private final CrsInfo crsInfo;

  public CqlVisitorMapEnvelopes(CrsInfo crsInfo) {
    this.crsInfo = crsInfo;
  }

  @Override
  public CqlNode visit(SpatialLiteral spatialLiteral, List<CqlNode> children) {
    if (spatialLiteral.getType() == Bbox.class && spatialLiteral.getValue() instanceof Bbox)
      return SpatialLiteral.of(
          ((GeometryNode) visit((Bbox) spatialLiteral.getValue(), children)).getGeometry());

    return super.visit(spatialLiteral, children);
  }

  @Override
  public CqlNode visit(Bbox bbox, List<CqlNode> children) {
    List<Double> c = bbox.getCoordinates();
    EpsgCrs crs = bbox.getCrs().orElse(OgcCrs.CRS84);

    // if the bbox is degenerate (vertical or horizontal line, point), reduce
    // the geometry
    if (c.get(0).equals(c.get(2)) && c.get(1).equals(c.get(3))) {
      return GeometryNode.of(Point.of(c.get(0), c.get(1), crs));
    } else if (c.get(0).equals(c.get(2)) || c.get(1).equals(c.get(3))) {
      return GeometryNode.of(
          LineString.of(
              PositionList.of(Axes.XY, new double[] {c.get(0), c.get(1), c.get(2), c.get(3)}),
              Optional.of(crs)));
    }

    // if the bbox crosses the antimeridian, we create a MultiPolygon with a polygon
    // on each side of the antimeridian
    if (Objects.nonNull(crsInfo)) {
      int axisWithWraparaound = crsInfo.getAxisWithWraparound(crs).orElse(-1);
      if (axisWithWraparaound == 0 && c.get(0) > c.get(2)) {
        // x axis is longitude
        return GeometryNode.of(
            MultiPolygon.of(
                List.of(
                    Polygon.of(
                        List.of(
                            PositionList.of(
                                Axes.XY,
                                new double[] {
                                  c.get(0), c.get(1), 180.0, c.get(1), 180.0, c.get(3), c.get(0),
                                  c.get(3), c.get(0), c.get(1)
                                })),
                        Optional.of(crs)),
                    Polygon.of(
                        List.of(
                            PositionList.of(
                                Axes.XY,
                                new double[] {
                                  -180, c.get(1), c.get(2), c.get(1), c.get(2), c.get(3), -180,
                                  c.get(3), -180, c.get(1)
                                })),
                        Optional.of(crs))),
                Optional.of(crs)));
      } else if (axisWithWraparaound == 1 && c.get(1) > c.get(3)) {
        // y axis is longitude
        return GeometryNode.of(
            MultiPolygon.of(
                List.of(
                    Polygon.of(
                        List.of(
                            PositionList.of(
                                Axes.XY,
                                new double[] {
                                  c.get(0), c.get(1), c.get(2), c.get(1), c.get(2), 180, c.get(0),
                                  180, c.get(0), c.get(1)
                                })),
                        Optional.of(crs)),
                    Polygon.of(
                        List.of(
                            PositionList.of(
                                Axes.XY,
                                new double[] {
                                  c.get(0), -180, c.get(2), -180, c.get(2), c.get(3), c.get(0),
                                  c.get(3), c.get(0), -180
                                })),
                        Optional.of(crs))),
                Optional.of(crs)));
      }
    }

    // standard case, convert to polygon
    return GeometryNode.of(
        Polygon.of(
            new double[] {
              c.get(0), c.get(1), c.get(2), c.get(1), c.get(2), c.get(3), c.get(0), c.get(3),
              c.get(0), c.get(1)
            },
            crs));
  }
}
