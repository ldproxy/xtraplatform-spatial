/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

import com.google.common.collect.ImmutableSet;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.CircularString;
import de.ii.xtraplatform.geometries.domain.CompoundCurve;
import de.ii.xtraplatform.geometries.domain.Curve;
import de.ii.xtraplatform.geometries.domain.CurvePolygon;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryCollection;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.MultiCurve;
import de.ii.xtraplatform.geometries.domain.MultiLineString;
import de.ii.xtraplatform.geometries.domain.MultiPoint;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.MultiSurface;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.PolyhedralSurface;
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import de.ii.xtraplatform.geometries.domain.transform.GeometryVisitor;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

// TODO add support for GML 2.1 and GML 3.1
public class GeometryEncoderGml implements GeometryVisitor<Void> {

  public static final String SPACE = " ";
  public static final String OPEN = "<";
  public static final String CLOSE = ">";
  public static final String EQUALS = "=";
  public static final String QUOTE = "\"";
  public static final String SLASH = "/";

  private static final String POINT = "Point";
  private static final String MULTI_POINT = "MultiPoint";
  private static final String LINE_STRING = "LineString";
  private static final String CURVE = "Curve";
  private static final String LINE_STRING_SEGMENT = "LineStringSegment";
  private static final String ARC = "Arc";
  private static final String ARC_STRING = "ArcString";
  private static final String MULTI_CURVE = "MultiCurve";
  private static final String MULTI_LINE_STRING = "MultiLineString";
  private static final String POLYGON = "Polygon";
  private static final String POLYGON_PATCH = "PolygonPatch";
  private static final String POLYHEDRAL_SURFACE = "PolyhedralSurface";
  private static final String MULTI_SURFACE = "MultiSurface";
  private static final String MULTI_GEOMETRY = "MultiGeometry";
  private static final String MULTI_POLYGON = "MultiPolygon";
  private static final String SOLID = "Solid";
  private static final String SHELL = "Shell";
  private static final String COMPOSITE_SURFACE = "CompositeSurface";
  private static final String COMPOSITE_CURVE = "CompositeCurve";
  private static final String POS = "pos";
  private static final String POS_LIST = "posList";
  private static final String ID = "id";
  private static final String SRS_NAME = "srsName";
  private static final String SEGMENTS = "segments";
  private static final String PATCHES = "patches";
  private static final String POINT_MEMBER = "pointMember";
  private static final String CURVE_MEMBER = "curveMember";
  private static final String LINEAR_RING = "LinearRing";
  private static final String RING = "Ring";
  private static final String EXTERIOR = "exterior";
  private static final String INTERIOR = "interior";
  private static final String SURFACE_MEMBER = "surfaceMember";
  private static final String GEOMETRY_MEMBER = "geometryMember";
  /*
  private static final Map<GmlVersion, Map<GeometryType, String>> GEOMETRY_ELEMENT =
      ImmutableMap.of(
          GML32,
          ImmutableMap.of(
              GeometryType.POINT, POINT,
              GeometryType.MULTI_POINT, MULTI_POINT,
              GeometryType.LINE_STRING, LINE_STRING,
              GeometryType.MULTI_LINE_STRING, MULTI_CURVE,
              GeometryType.POLYGON, POLYGON,
              GeometryType.MULTI_POLYGON, MULTI_SURFACE,
              GeometryType.GEOMETRY_COLLECTION, MULTI_GEOMETRY),
          GML31,
          ImmutableMap.of(
              GeometryType.POINT, "gml31" + POINT.substring(3),
              GeometryType.MULTI_POINT, "gml31" + MULTI_POINT.substring(3),
              GeometryType.LINE_STRING, "gml31" + LINE_STRING.substring(3),
              GeometryType.MULTI_LINE_STRING, "gml31" + MULTI_CURVE.substring(3),
              GeometryType.POLYGON, "gml31" + POLYGON.substring(3),
              GeometryType.MULTI_POLYGON, "gml31" + MULTI_SURFACE.substring(3),
              GeometryType.GEOMETRY_COLLECTION, "gml31" + MULTI_GEOMETRY.substring(3)),
          GML21,
          ImmutableMap.of(
              GeometryType.POINT, "gml21" + POINT.substring(3),
              GeometryType.MULTI_POINT, "gml21" + MULTI_POINT.substring(3),
              GeometryType.LINE_STRING, "gml21" + LINE_STRING.substring(3),
              GeometryType.MULTI_LINE_STRING, MULTI_LINE_STRING.substring(3),
              GeometryType.POLYGON, "gml21" + POLYGON.substring(3),
              GeometryType.MULTI_POLYGON, MULTI_POLYGON.substring(3),
              GeometryType.GEOMETRY_COLLECTION, "gml21" + MULTI_GEOMETRY.substring(3)));
  */
  private static final String OUTER_BOUNDARY_IS = "gml21:outerBoundaryIs";
  private static final String INNER_BOUNDARY_IS = "gml21:innerBoundaryIs";
  private static final String LINE_STRING_MEMBER = "gml21:lineStringMember";
  private static final String COORDINATES = "gml21:coordinates";
  private static final String POLYGON_MEMBER = "gml21:polygonMember";

  public enum Options {
    WITH_GML_ID,
    WITH_SRS_NAME,
    LINE_STRING_AS_SEGMENT,
    POLYGON_AS_PATCH
  }

  private final StringBuilder builder;
  private final Optional<String> gmlPrefix;
  private final String gmlIdPrefix;
  private final Set<GeometryEncoderGml.Options> options;
  private final int[] precision;
  private final Optional<GeometryEncoderGml> encodeAsSegmentOrPatch;
  private final Optional<GeometryEncoderGml> encodeAsEmbeddedGeometry;
  private int nextGmlId = 0;
  private String srsName;

  public GeometryEncoderGml(StringBuilder builder) {
    this.builder = builder;
    this.gmlPrefix = Optional.of("gml");
    this.gmlIdPrefix = "geom_";
    this.options = Set.of();
    this.precision = null;
    this.encodeAsSegmentOrPatch =
        Optional.of(
            new GeometryEncoderGml(
                builder,
                Set.of(Options.LINE_STRING_AS_SEGMENT, Options.POLYGON_AS_PATCH),
                this.gmlPrefix,
                Optional.empty(),
                List.of()));
    this.encodeAsEmbeddedGeometry =
        Optional.of(
            new GeometryEncoderGml(builder, Set.of(), this.gmlPrefix, Optional.empty(), List.of()));
    this.srsName = null;
  }

  public GeometryEncoderGml(
      StringBuilder builder,
      Set<GeometryEncoderGml.Options> options,
      Optional<String> gmlPrefix,
      Optional<String> gmlIdPrefix,
      List<Integer> precision) {
    this.builder = builder;
    this.gmlPrefix = gmlPrefix;
    this.gmlIdPrefix = gmlIdPrefix.orElse("geom_");
    this.options = options;
    this.encodeAsSegmentOrPatch =
        options.contains(Options.LINE_STRING_AS_SEGMENT)
                && options.contains(Options.POLYGON_AS_PATCH)
            ? Optional.empty()
            : Optional.of(
                new GeometryEncoderGml(
                    builder,
                    ImmutableSet.<Options>builder()
                        .addAll(options)
                        .add(Options.LINE_STRING_AS_SEGMENT, Options.POLYGON_AS_PATCH)
                        .build(),
                    gmlPrefix,
                    gmlIdPrefix,
                    precision));
    this.encodeAsEmbeddedGeometry =
        options.contains(Options.WITH_SRS_NAME)
            ? Optional.of(
                new GeometryEncoderGml(
                    builder,
                    ImmutableSet.<Options>builder()
                        .addAll(
                            options.stream()
                                .filter(option -> option != Options.WITH_SRS_NAME)
                                .toList())
                        .build(),
                    gmlPrefix,
                    gmlIdPrefix,
                    precision))
            : Optional.empty();
    this.precision =
        precision.stream().anyMatch(v -> v > 0)
            ? precision.stream().mapToInt(v -> v).toArray()
            : null;
    this.srsName = null;
  }

  @Override
  public Optional<Void> initAndCheckGeometry(Geometry<?> geometry) {
    if (srsName == null) {
      srsName =
          geometry
              .getCrs()
              .orElse(
                  geometry.getAxes() == Axes.XY || geometry.getAxes() == Axes.XYM
                      ? OgcCrs.CRS84
                      : OgcCrs.CRS84h)
              .toUriString();
    }

    return Optional.empty();
  }

  private void write(String s) {
    builder.append(s);
  }

  private void write(double d) {
    builder.append(d);
  }

  private void write(BigDecimal d) {
    builder.append(d.toString());
  }

  private void writeAttribute(String name, String value) {
    write(name);
    write(EQUALS);
    write(QUOTE);
    write(value);
    write(QUOTE);
  }

  private void writeStartTagObject(String tagName, boolean suppressSrsName) {
    writeStartTag(tagName, Map.of(), true, suppressSrsName);
  }

  private void writeStartTagDataType(String tagName) {
    writeStartTag(tagName, Map.of(), false, true);
  }

  private void writeStartTagProperty(String tagName) {
    writeStartTag(tagName, Map.of(), false, true);
  }

  private void writeStartTag(
      String tagName, Map<String, String> attributes, boolean isObject, boolean suppressSrsName) {
    write(OPEN);
    gmlPrefix.ifPresent(pre -> write(pre + ':'));
    write(tagName);
    if (isObject) {
      if (options.contains(Options.WITH_GML_ID)) {
        write(SPACE);
        writeAttribute(gmlPrefix.map(pre -> pre + ':' + ID).orElse(ID), gmlIdPrefix + nextGmlId++);
      }
      if (options.contains(Options.WITH_SRS_NAME) && !suppressSrsName) {
        write(SPACE);
        writeAttribute(SRS_NAME, srsName);
      }
    }
    attributes.forEach(
        (key, value) -> {
          write(SPACE);
          writeAttribute(key, value);
        });
    write(CLOSE);
  }

  private void writeEndTag(String tagName) {
    write(OPEN);
    write(SLASH);
    gmlPrefix.ifPresent(pre -> write(pre + ':'));
    write(tagName);
    write(CLOSE);
  }

  private void writeCoordinates(double[] coordinates, Axes axes) {
    int dimensionWithoutM = axes == Axes.XY || axes == Axes.XYM ? 2 : 3;
    for (int i = 0; i < coordinates.length / axes.size(); i++) {
      for (int j = 0; j < dimensionWithoutM; j++) {
        if (i > 0 || j > 0) {
          write(SPACE);
        }
        if (precision != null && j < precision.length && precision[j] > 0) {
          write(
              BigDecimal.valueOf(coordinates[i * dimensionWithoutM + j])
                  .setScale(precision[j], RoundingMode.HALF_UP));
        } else {
          write(coordinates[i * dimensionWithoutM + j]);
        }
      }
    }
  }

  private void writePosition(double[] coordinates, Axes axes) {
    writeStartTagProperty(POS);
    writeCoordinates(coordinates, axes);
    writeEndTag(POS);
  }

  private void writePositionList(double[] coordinates, Axes axes) {
    writeStartTagProperty(POS_LIST);
    writeCoordinates(coordinates, axes);
    writeEndTag(POS_LIST);
  }

  @Override
  public Void visit(Point geometry) {
    writeStartTagObject(POINT, false);
    writePosition(geometry.getValue().getCoordinates(), geometry.getAxes());
    writeEndTag(POINT);
    return null;
  }

  @Override
  public Void visit(MultiPoint geometry) {
    writeStartTagObject(MULTI_POINT, false);
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      Point point = geometry.getValue().get(i);
      writeStartTagProperty(POINT_MEMBER);
      point.accept(encodeAsEmbeddedGeometry.orElse(this));
      writeEndTag(POINT_MEMBER);
    }
    writeEndTag(MULTI_POINT);
    return null;
  }

  @Override
  public Void visit(SingleCurve geometry) {
    if (geometry instanceof LineString lineString) {
      writeLineString(lineString, options.contains(Options.LINE_STRING_AS_SEGMENT));
    } else if (geometry instanceof CircularString circularString) {
      writeCircularString(circularString, options.contains(Options.LINE_STRING_AS_SEGMENT));
    }
    return null;
  }

  private void writeLineString(LineString geometry, boolean asSegment) {
    String tagName;
    if (asSegment) {
      tagName = LINE_STRING_SEGMENT;
      writeStartTagDataType(tagName);
    } else {
      tagName = LINE_STRING;
      writeStartTagObject(tagName, false);
    }
    writePositionList(geometry.getValue().getCoordinates(), geometry.getAxes());
    writeEndTag(tagName);
  }

  private void writeCircularString(CircularString geometry, boolean asSegment) {
    if (!asSegment) {
      writeStartTagObject(CURVE, false);
      writeStartTagProperty(SEGMENTS);
    }
    String tagName = geometry.getValue().getNumPositions() == 3 ? ARC : ARC_STRING;
    writeStartTagDataType(tagName);
    writePositionList(geometry.getValue().getCoordinates(), geometry.getAxes());
    writeEndTag(tagName);
    if (!asSegment) {
      writeEndTag(SEGMENTS);
      writeEndTag(CURVE);
    }
  }

  @Override
  public Void visit(MultiLineString geometry) {
    writeStartTagObject(MULTI_CURVE, false);
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      LineString lineString = geometry.getValue().get(i);
      writeStartTagProperty(CURVE_MEMBER);
      lineString.accept(encodeAsEmbeddedGeometry.orElse(this));
      writeEndTag(CURVE_MEMBER);
    }
    writeEndTag(MULTI_CURVE);
    return null;
  }

  @Override
  public Void visit(Polygon geometry) {
    boolean asPatch = options.contains(Options.POLYGON_AS_PATCH);
    if (asPatch) {
      writeStartTagDataType(POLYGON_PATCH);
    } else {
      writeStartTagObject(POLYGON, false);
    }
    for (int i = 0; i < geometry.getNumRings(); i++) {
      LineString ring = geometry.getValue().get(i);
      if (i == 0) {
        writeStartTagProperty(EXTERIOR);
      } else {
        writeStartTagProperty(INTERIOR);
      }
      writeStartTagObject(LINEAR_RING, true);
      writePositionList(ring.getValue().getCoordinates(), geometry.getAxes());
      writeEndTag(LINEAR_RING);
      if (i == 0) {
        writeEndTag(EXTERIOR);
      } else {
        writeEndTag(INTERIOR);
      }
    }
    if (asPatch) {
      writeEndTag(POLYGON_PATCH);
    } else {
      writeEndTag(POLYGON);
    }
    return null;
  }

  @Override
  public Void visit(MultiPolygon geometry) {
    writeStartTagObject(MULTI_SURFACE, false);
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      Polygon polygon = geometry.getValue().get(i);
      writeStartTagProperty(SURFACE_MEMBER);
      polygon.accept(encodeAsEmbeddedGeometry.orElse(this));
      writeEndTag(SURFACE_MEMBER);
    }
    writeEndTag(MULTI_SURFACE);
    return null;
  }

  @Override
  public Void visit(MultiCurve geometry) {
    writeStartTagObject(MULTI_CURVE, false);
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      Geometry<?> geometry2 = geometry.getValue().get(i);
      writeStartTagProperty(CURVE_MEMBER);
      geometry2.accept(encodeAsEmbeddedGeometry.orElse(this));
      writeEndTag(CURVE_MEMBER);
    }
    writeEndTag(MULTI_CURVE);
    return null;
  }

  @Override
  public Void visit(MultiSurface geometry) {
    writeStartTagObject(MULTI_SURFACE, false);
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      Geometry<?> geometry2 = geometry.getValue().get(i);
      writeStartTagProperty(SURFACE_MEMBER);
      geometry2.accept(encodeAsEmbeddedGeometry.orElse(this));
      writeEndTag(SURFACE_MEMBER);
    }
    writeEndTag(MULTI_SURFACE);
    return null;
  }

  @Override
  public Void visit(GeometryCollection geometry) {
    writeStartTagObject(MULTI_GEOMETRY, false);
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      Geometry<?> geometry2 = geometry.getValue().get(i);
      writeStartTagProperty(GEOMETRY_MEMBER);
      geometry2.accept(encodeAsEmbeddedGeometry.orElse(this));
      writeEndTag(GEOMETRY_MEMBER);
    }
    writeEndTag(MULTI_GEOMETRY);
    return null;
  }

  @Override
  public Void visit(CompoundCurve geometry) {
    writeStartTagObject(CURVE, false);
    writeStartTagProperty(SEGMENTS);
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      SingleCurve curve = geometry.getValue().get(i);
      curve.accept(encodeAsSegmentOrPatch.orElse(this));
    }
    writeEndTag(SEGMENTS);
    writeEndTag(CURVE);
    return null;
  }

  @Override
  public Void visit(CurvePolygon geometry) {
    writeStartTagObject(POLYGON, false);
    for (int i = 0; i < geometry.getNumRings(); i++) {
      Curve<?> ring = geometry.getValue().get(i);
      if (i == 0) {
        writeStartTagProperty(EXTERIOR);
      } else {
        writeStartTagProperty(INTERIOR);
      }
      writeStartTagObject(RING, false);
      writeStartTagProperty(CURVE_MEMBER);
      ring.accept(encodeAsEmbeddedGeometry.orElse(this));
      writeEndTag(CURVE_MEMBER);
      writeEndTag(RING);
      if (i == 0) {
        writeEndTag(EXTERIOR);
      } else {
        writeEndTag(INTERIOR);
      }
    }
    writeEndTag(POLYGON);
    return null;
  }

  @Override
  public Void visit(PolyhedralSurface geometry) {
    if (geometry.isClosed()) {
      writeStartTagObject(SOLID, false);
      writeStartTagProperty(EXTERIOR);
      writeStartTagObject(SHELL, true);
      for (int i = 0; i < geometry.getNumPolygons(); i++) {
        writeStartTagProperty(SURFACE_MEMBER);
        Polygon polygon = geometry.getValue().get(i);
        polygon.accept(encodeAsEmbeddedGeometry.orElse(this));
        writeEndTag(SURFACE_MEMBER);
      }
      writeEndTag(SHELL);
      writeEndTag(EXTERIOR);
      writeEndTag(SOLID);
    } else {
      writeStartTagObject(POLYHEDRAL_SURFACE, false);
      writeStartTagProperty(PATCHES);
      for (int i = 0; i < geometry.getNumPolygons(); i++) {
        Polygon polygon = geometry.getValue().get(i);
        polygon.accept(encodeAsSegmentOrPatch.orElse(this));
      }
      writeEndTag(PATCHES);
      writeEndTag(POLYHEDRAL_SURFACE);
    }
    return null;
  }
}
