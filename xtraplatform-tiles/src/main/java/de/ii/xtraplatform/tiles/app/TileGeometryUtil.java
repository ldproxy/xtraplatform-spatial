/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.tiles.app;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;

public final class TileGeometryUtil {

  private TileGeometryUtil() {}

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  public static Geometry getTileGeometry(
      Geometry geom,
      AffineTransformation affineTransformation,
      Geometry clipGeometry,
      PrecisionModel precisionModel,
      double minimumSizeInPixel) {

    // The following changes are applied:
    // 1. The coordinates are converted to the tile coordinate system (0/0 is top left, 256/256 is
    //    bottom right).
    // 2. Try to fix invalid geometries.
    // 3. Small rings or line strings are dropped (small in the context of the tile, one pixel or
    //    less). The idea is to simply drop them as early as possible and before the next processing
    //    steps which may depend on having valid geometries and removing everything that will
    //    eventually be removed anyway helps.
    // 4. Remove unnecessary vertices and snap coordinates to the grid.
    // 5. If the resulting geometry is invalid polygonal geometry, try to make it valid.
    // 6. Hopefully we have a valid geometry now, so try to clip it to the tile.
    //
    // After each step, check, if we still have a geometry or the resulting tile geometry was too
    // small for
    // the tile. In that case the feature is ignored.

    // 1 convert to the tile coordinate system
    geom.apply(affineTransformation);

    Geometry result = geom;

    // 2 fix invalid source geometries
    if (!result.isValid()) {
      result = new GeometryFixer(result).getResult();
      if (Objects.isNull(result) || result.isEmpty()) {
        return null;
      }
    }

    // 3 remove small rings or line strings (small in the context of the tile)
    result = removeSmallPieces(result, minimumSizeInPixel);
    if (Objects.isNull(result) || result.isEmpty()) {
      return null;
    }

    // 4 simplify the geometry
    result = TopologyPreservingSimplifier.simplify(result, 1.0 / precisionModel.getScale());
    if (Objects.isNull(result) || result.isEmpty()) {
      return null;
    }

    // 5 reduce the geometry to the tile grid
    result = GeometryPrecisionReducer.reducePointwise(result, precisionModel);
    if (Objects.isNull(result) || result.isEmpty()) {
      return null;
    }

    // 6 if the resulting geometry is invalid, try to make it valid and ensure it is still aligned
    //   with the tile grid; give up, if it is still invalid after two attempts
    for (int count = 0; !result.isValid() && count < 2; count++) {
      result = GeometryFixer.fix(result);
      if (Objects.isNull(result) || result.isEmpty()) {
        return null;
      }

      result = GeometryPrecisionReducer.reducePointwise(result, precisionModel);
      if (Objects.isNull(result) || result.isEmpty()) {
        return null;
      }
    }

    // 7 limit the coordinates to the tile with a buffer
    result = clipGeometry(result, clipGeometry);
    if (Objects.isNull(result) || result.isEmpty()) {
      return null;
    }

    return result;
  }

  static List<Polygon> splitMultiPolygon(MultiPolygon geom) {
    List<Polygon> patches = new ArrayList<>();
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      patches.add((Polygon) geom.getGeometryN(i));
    }
    return patches;
  }

  static List<LineString> splitMultiLineString(MultiLineString geom) {
    List<LineString> segments = new ArrayList<>();
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      segments.add((LineString) geom.getGeometryN(i));
    }
    return segments;
  }

  private static Geometry clipGeometry(Geometry geometry, Geometry clipGeometry) {
    Geometry result = geometry;
    try {
      result = clipGeometry.intersection(geometry);

      // sometimes an intersection is returned as an empty geometry.
      // going via wkt fixes the problem.
      if (result.isEmpty() && geometry.intersects(clipGeometry)) {
        Geometry originalViaWkt = new WKTReader().read(geometry.toText());
        result = clipGeometry.intersection(originalViaWkt);
      }

    } catch (TopologyException | ParseException e) {
      // could not intersect or encode/decode WKT. original geometry will be used instead.
    }
    return result;
  }

  private static Polygon removeSmallPieces(Polygon geom, double minimumSizeInPixel) {
    if (geom.getArea() < minimumSizeInPixel * minimumSizeInPixel) {
      // skip this feature, too small
      return null;
    }
    List<LinearRing> holes = new ArrayList<>();
    boolean skipped = false;
    for (int i = 0; i < geom.getNumInteriorRing(); i++) {
      LinearRing hole = geom.getInteriorRingN(i);
      if (geom.getFactory().createPolygon(hole).getArea()
          >= minimumSizeInPixel * minimumSizeInPixel) {
        holes.add(hole);
      } else {
        skipped = true;
      }
    }

    return skipped
        ? geom.getFactory().createPolygon(geom.getExteriorRing(), holes.toArray(LinearRing[]::new))
        : geom;
  }

  @SuppressWarnings({"PMD.CognitiveComplexity", "PMD.CyclomaticComplexity"})
  private static Geometry removeSmallPieces(Geometry geom, double minimumSizeInPixel) {
    if (geom instanceof Polygon) {
      return removeSmallPieces((Polygon) geom, minimumSizeInPixel);
    } else if (geom instanceof MultiPolygon) {
      List<Polygon> patches = new ArrayList<>();
      boolean changed = false;
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        Polygon patch = (Polygon) geom.getGeometryN(i);
        Polygon newPolygon = removeSmallPieces(patch, minimumSizeInPixel);
        if (Objects.isNull(newPolygon)) {
          changed = true;
          continue;
        }
        if (!Objects.equals(patch, newPolygon)) {
          changed = true;
        }
        patches.add(newPolygon);
      }
      return changed ? geom.getFactory().createMultiPolygon(patches.toArray(Polygon[]::new)) : geom;
    } else if (geom instanceof LineString) {
      if (geom.getLength() < minimumSizeInPixel) {
        // skip this feature, too small
        return null;
      }
    } else if (geom instanceof MultiLineString) {
      List<LineString> segments = new ArrayList<>();
      boolean changed = false;
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        LineString segment = (LineString) geom.getGeometryN(i);
        if (segment.getLength() >= minimumSizeInPixel) {
          segments.add(segment);
        } else {
          // skip this feature, too small
          changed = true;
        }
      }
      return changed
          ? geom.getFactory().createMultiLineString(segments.toArray(LineString[]::new))
          : geom;
    }

    return geom;
  }
}
