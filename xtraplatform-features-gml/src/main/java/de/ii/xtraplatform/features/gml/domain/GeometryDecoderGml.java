/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLStreamReader;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.geometries.domain.ImmutableLineString;
import de.ii.xtraplatform.geometries.domain.ImmutableMultiLineString;
import de.ii.xtraplatform.geometries.domain.ImmutableMultiPoint;
import de.ii.xtraplatform.geometries.domain.ImmutableMultiPolygon;
import de.ii.xtraplatform.geometries.domain.ImmutablePoint;
import de.ii.xtraplatform.geometries.domain.ImmutablePolygon;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.MultiLineString;
import de.ii.xtraplatform.geometries.domain.MultiPoint;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.transcode.AbstractGeometryDecoder;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

public class GeometryDecoderGml extends AbstractGeometryDecoder {

  // TODO: add more GML geometry types if needed, currently only the most common ones are supported
  // TODO: support GML 2.1 and GML 3.1

  static final List<String> GEOMETRY_PARTS =
      new ImmutableList.Builder<String>()
          .add("pointMember")
          .add("curveMember")
          .add("surfaceMember")
          .build();
  static final List<String> GEOMETRY_COORDINATES =
      new ImmutableList.Builder<String>().add("posList").add("pos").add("coordinates").build();

  static class PartialGeometry {
    PartialGeometry parent = null;
    Deque<PartialGeometry> children = new ArrayDeque<>();
    boolean isComplete = false;
    Geometry<?> geometry = null;
    double[] coordinates = null;
    StringBuilder textBuffer = new StringBuilder();
  }

  private boolean waitingForInput = false;
  private boolean waitingForGeometry = true;
  private PartialGeometry currentGeometry = null;

  public GeometryDecoderGml() {}

  public Optional<Geometry<?>> decode(
      AsyncXMLStreamReader<AsyncByteArrayFeeder> parser,
      Optional<EpsgCrs> crs,
      OptionalInt srsDimension)
      throws XMLStreamException, IOException {
    return decode(parser, crs, srsDimension, false);
  }

  public Optional<Geometry<?>> decode(
      AsyncXMLStreamReader<AsyncByteArrayFeeder> parser,
      Optional<EpsgCrs> defaultCrs,
      OptionalInt srsDimension,
      boolean useCurrentEvent)
      throws XMLStreamException, IOException {
    String localName;
    waitingForInput = false;
    boolean doNotAdvance = useCurrentEvent;
    while (doNotAdvance || parser.hasNext()) {
      int event = doNotAdvance ? parser.getEventType() : parser.next();
      doNotAdvance = false;
      switch (event) {
        case AsyncXMLStreamReader.EVENT_INCOMPLETE:
          waitingForInput = true;
          return Optional.empty();
        case XMLStreamConstants.START_ELEMENT:
          localName = parser.getLocalName();
          if (waitingForGeometry) {
            Optional<EpsgCrs> crs = getCrsFromSrsName(parser).or(() -> defaultCrs);
            Axes axes =
                getDimFromSrsDimension(parser).orElse(srsDimension.orElse(2)) == 2
                    ? Axes.XY
                    : Axes.XYZ;
            Geometry<?> geom =
                switch (localName) {
                  case "Point" -> ImmutablePoint.builder()
                      .crs(crs)
                      .value(Position.empty(axes))
                      .build();
                  case "LineString" -> ImmutableLineString.builder()
                      .crs(crs)
                      .value(PositionList.empty(axes))
                      .build();
                  case "Polygon" -> ImmutablePolygon.builder().crs(crs).axes(axes).build();
                  case "MultiPoint" -> ImmutableMultiPoint.builder().crs(crs).axes(axes).build();
                  case "MultiCurve" -> ImmutableMultiLineString.builder()
                      .crs(crs)
                      .axes(axes)
                      .build();
                  case "MultiSurface" -> ImmutableMultiPolygon.builder()
                      .crs(crs)
                      .axes(axes)
                      .build();
                  default -> throw new IOException("Unsupported GML geometry type: " + localName);
                };
            if (currentGeometry == null) {
              currentGeometry = new PartialGeometry();
            } else {
              PartialGeometry parent = currentGeometry;
              currentGeometry = new PartialGeometry();
              currentGeometry.parent = parent;
              parent.children.add(currentGeometry);
            }
            currentGeometry.geometry = geom;
            waitingForGeometry = false;
          } else if (GEOMETRY_COORDINATES.contains(localName)) {
            if (currentGeometry == null) {
              throw new IllegalStateException(
                  "No geometry started before <gml:" + localName + "> element.");
            }
            if (currentGeometry.isComplete) {
              throw new IllegalStateException(
                  "<gml:" + localName + "> element cannot be added to a completed geometry.");
            }
            double[] coords = parseCoordinates(parser, localName);
            if (waitingForInput) {
              return Optional.empty();
            }
            handleCoordinates(localName, coords);
          } else if (GEOMETRY_PARTS.contains(localName)) {
            if (currentGeometry == null) {
              throw new IllegalStateException(
                  "No geometry started before <gml:" + localName + "> element.");
            }
            if (currentGeometry.isComplete) {
              throw new IllegalStateException(
                  "<gml:" + localName + "> element cannot be added to a completed geometry.");
            }
            waitingForGeometry = true;
          }
          break;
        case XMLStreamConstants.END_ELEMENT:
          localName = parser.getLocalName();
          if (currentGeometry == null) {
            throw new IllegalStateException(
                "No geometry started before </gml:" + localName + "> element.");
          }
          if ("Point".equals(localName)
              || "LineString".equals(localName)
              || "Polygon".equals(localName)
              || "MultiPoint".equals(localName)
              || "MultiCurve".equals(localName)
              || "MultiSurface".equals(localName)) {
            if (currentGeometry.isComplete) {
              throw new IllegalStateException(
                  "Geometry already completed for </gml:" + localName + "> element.");
            }
            switch (localName) {
              case "MultiPoint" -> {
                var builder =
                    ImmutableMultiPoint.builder().from((MultiPoint) currentGeometry.geometry);
                for (PartialGeometry child : currentGeometry.children) {
                  builder.addValue((Point) child.geometry);
                }
                currentGeometry.geometry = builder.build();
              }
              case "MultiCurve" -> {
                var builder =
                    ImmutableMultiLineString.builder()
                        .from((MultiLineString) currentGeometry.geometry);
                for (PartialGeometry child : currentGeometry.children) {
                  builder.addValue((LineString) child.geometry);
                }
                currentGeometry.geometry = builder.build();
              }
              case "MultiSurface" -> {
                var builder =
                    ImmutableMultiPolygon.builder().from((MultiPolygon) currentGeometry.geometry);
                for (PartialGeometry child : currentGeometry.children) {
                  builder.addValue((Polygon) child.geometry);
                }
                currentGeometry.geometry = builder.build();
              }
            }
            currentGeometry.isComplete = true;
            if (currentGeometry.geometry.isEmpty()) {
              throw new IllegalStateException(
                  "Geometry is empty for </gml:" + localName + "> element.");
            }
            if (currentGeometry.parent != null) {
              currentGeometry = currentGeometry.parent;
            } else {
              Geometry<?> geom = currentGeometry.geometry;
              currentGeometry = null;
              waitingForGeometry = true;
              waitingForInput = false;
              return Optional.of(geom);
            }
          }
          break;
        case XMLStreamConstants.CHARACTERS:
          break;
      }
    }
    throw new IOException("Unexpected end of XML stream, no complete geometry found.");
  }

  private void handleCoordinates(String localName, double[] coords) {
    Geometry<?> geom = currentGeometry.geometry;
    GeometryType geomType = geom.getType();
    Axes axes = geom.getAxes();
    PartialGeometry child;
    switch (geomType) {
      case POINT:
        if ("posList".equals(localName)) {
          throw new IllegalStateException("<gml:posList> is not allowed for Point coordinates.");
        }
        if (currentGeometry.coordinates != null) {
          addCoordinates(coords);
        }
        Position position =
            Position.of(coords.length == 3 ? Axes.XYZ : Axes.XY, currentGeometry.coordinates);
        currentGeometry.geometry = ImmutablePoint.copyOf((Point) geom).withValue(position);
        break;
      case LINE_STRING:
        addCoordinates(coords);
        if (!"pos".equals(localName)) {
          LineString lineString = (LineString) geom;
          PositionList positionList = PositionList.of(axes, currentGeometry.coordinates);
          currentGeometry.geometry = ImmutableLineString.copyOf(lineString).withValue(positionList);
          currentGeometry.coordinates = null;
        }
        break;
      case POLYGON:
        addCoordinates(coords);
        if (!"pos".equals(localName)) {
          Polygon polygon = (Polygon) geom;
          PositionList positionList = PositionList.of(axes, currentGeometry.coordinates);
          currentGeometry.geometry =
              ImmutablePolygon.copyOf(polygon)
                  .withValue(
                      ImmutableList.<LineString>builder()
                          .addAll(polygon.getValue())
                          .add(LineString.of(positionList, polygon.getCrs()))
                          .build());
          currentGeometry.coordinates = null;
        }
        break;
      case MULTI_POINT:
        Position pos = Position.of(coords.length == 3 ? Axes.XYZ : Axes.XY, coords);
        Point point = ImmutablePoint.builder().crs(geom.getCrs()).value(pos).build();
        child = new PartialGeometry();
        child.parent = currentGeometry;
        child.geometry = point;
        currentGeometry.children.add(child);
        break;
      case MULTI_LINE_STRING:
        PositionList posList = PositionList.of(axes, coords);
        LineString lineString =
            ImmutableLineString.builder().crs(geom.getCrs()).value(posList).build();
        child = new PartialGeometry();
        child.parent = currentGeometry;
        child.geometry = lineString;
        currentGeometry.children.add(child);
        break;
      case MULTI_POLYGON:
        PositionList ring = PositionList.of(axes, coords);
        Polygon polygon =
            ImmutablePolygon.builder()
                .crs(geom.getCrs())
                .axes(axes)
                .addValue(LineString.of(ring, geom.getCrs()))
                .build();
        child = new PartialGeometry();
        child.parent = currentGeometry;
        child.geometry = polygon;
        currentGeometry.children.add(child);
        break;
      default:
        throw new IllegalStateException("Unsupported geometry type: " + geomType);
    }
  }

  public Optional<Geometry<?>> continueDecoding(
      AsyncXMLStreamReader<AsyncByteArrayFeeder> parser,
      Optional<EpsgCrs> defaultCrs,
      OptionalInt srsDimension,
      String localName,
      String bufferedText)
      throws XMLStreamException, IOException {
    if (GEOMETRY_COORDINATES.contains(localName)) {
      if (currentGeometry == null) {
        throw new IllegalStateException(
            "No geometry started before <gml:" + localName + "> element.");
      }
      if (currentGeometry.isComplete) {
        throw new IllegalStateException(
            "<gml:" + localName + "> element cannot be added to a completed geometry.");
      }
      double[] coords = parseDoubles(currentGeometry.textBuffer.append(bufferedText).toString());
      currentGeometry.textBuffer.setLength(0);
      handleCoordinates(localName, coords);
      return decode(parser, defaultCrs, srsDimension, false);
    }
    return decode(parser, defaultCrs, srsDimension, true);
  }

  private void addCoordinates(double[] coords) {
    if (currentGeometry.coordinates == null) {
      currentGeometry.coordinates = coords;
    } else {
      double[] newCoords = new double[currentGeometry.coordinates.length + coords.length];
      System.arraycopy(
          currentGeometry.coordinates, 0, newCoords, 0, currentGeometry.coordinates.length);
      System.arraycopy(coords, 0, newCoords, currentGeometry.coordinates.length, coords.length);
      currentGeometry.coordinates = newCoords;
    }
  }

  public boolean isWaitingForInput() {
    return waitingForInput;
  }

  private Optional<EpsgCrs> getCrsFromSrsName(AsyncXMLStreamReader<AsyncByteArrayFeeder> parser) {
    String srsName = parser.getAttributeValue(null, "srsName");
    if (srsName != null) {
      if (srsName.startsWith("urn:ogc:def:crs:EPSG::")) {
        String code = srsName.substring(srsName.lastIndexOf(":") + 1);
        try {
          return Optional.of(EpsgCrs.of(Integer.parseInt(code)));
        } catch (Exception e) {
          // ignore, fallback to default
        }
      } else if (srsName.startsWith("http://www.opengis.net/def/crs/EPSG/0/")) {
        String code = srsName.substring(srsName.lastIndexOf("/") + 1);
        try {
          return Optional.of(EpsgCrs.of(Integer.parseInt(code)));
        } catch (Exception e) {
          // ignore, fallback to default
        }
      } else if (srsName.equals("http://www.opengis.net/def/crs/OGC/0/CRS84")
          || srsName.equals("http://www.opengis.net/def/crs/OGC/1.3/CRS84")
          || srsName.equals("urn:ogc:def:crs:OGC:1.3:CRS84")) {
        return Optional.of(OgcCrs.CRS84);
      } else if (srsName.equals("http://www.opengis.net/def/crs/OGC/0/CRS84h")
          || srsName.equals("urn:ogc:def:crs:OGC::CRS84h")) {
        return Optional.of(OgcCrs.CRS84h);
      }
    }

    // get CRS from parent scope
    PartialGeometry g = currentGeometry;
    while (g != null) {
      if (g.geometry != null && g.geometry.getCrs().isPresent()) {
        return g.geometry.getCrs();
      }
      g = g.parent;
    }
    return Optional.empty();
  }

  private OptionalInt getDimFromSrsDimension(AsyncXMLStreamReader<AsyncByteArrayFeeder> parser) {
    String srsDimension = parser.getAttributeValue(null, "srsDimension");
    if (srsDimension != null) {
      try {
        return OptionalInt.of(Integer.parseInt(srsDimension));
      } catch (Exception e) {
        // ignore, fallback to default
      }
    }

    // get dimension from parent scope
    PartialGeometry g = currentGeometry;
    while (g != null) {
      if (g.geometry != null) {
        return OptionalInt.of(g.geometry.getAxes().size());
      }
      g = g.parent;
    }

    return OptionalInt.empty();
  }

  private double[] parseCoordinates(
      AsyncXMLStreamReader<AsyncByteArrayFeeder> parser, String tagName) throws XMLStreamException {
    StringBuilder text = new StringBuilder();
    while (parser.hasNext()) {
      int event = parser.next();
      if (event == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
        waitingForInput = true;
        currentGeometry.textBuffer = new StringBuilder(text);
        // keep current state and resume processing when more input is available
        return null;
      } else if (event == XMLStreamConstants.CHARACTERS) {
        text.append(parser.getText());
      } else if (event == XMLStreamConstants.END_ELEMENT && tagName.equals(parser.getLocalName())) {
        break;
      }
    }
    return parseDoubles(text.toString());
  }

  private static double[] parseDoubles(String text) {
    String[] parts = text.trim().split("[ \n\r\t]+");
    double[] coords = new double[parts.length];
    for (int i = 0; i < parts.length; i++) {
      coords[i] = Double.parseDouble(parts[i]);
    }
    return coords;
  }
}
