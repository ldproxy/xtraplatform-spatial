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
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import de.ii.xtraplatform.geometries.domain.Surface;
import de.ii.xtraplatform.geometries.domain.transcode.AbstractGeometryDecoder;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

public class GeometryDecoderGml extends AbstractGeometryDecoder {

  private enum Kind {
    POINT,
    LINE_STRING,
    POLYGON,
    CURVE,
    COMPOSITE_CURVE,
    SURFACE,
    COMPOSITE_SURFACE,
    POLYHEDRAL_SURFACE,
    SOLID,
    MULTI_POINT,
    MULTI_LINE_STRING,
    MULTI_CURVE,
    MULTI_POLYGON,
    MULTI_SURFACE,
    MULTI_GEOMETRY,
    POINT_MEMBER,
    CURVE_MEMBER,
    LINE_STRING_MEMBER,
    POLYGON_MEMBER,
    SURFACE_MEMBER,
    GEOMETRY_MEMBER,
    POINT_MEMBERS,
    CURVE_MEMBERS,
    POLYGON_MEMBERS,
    SURFACE_MEMBERS,
    EXTERIOR,
    INTERIOR,
    OUTER_BOUNDARY_IS,
    INNER_BOUNDARY_IS,
    SHELL,
    SEGMENTS,
    PATCHES,
    LINEAR_RING,
    RING,
    POLYGON_PATCH,
    LINE_STRING_SEGMENT,
    ARC,
    ARC_STRING,
    CIRCLE,
    POS,
    POS_LIST,
    COORDINATES,
    UNKNOWN
  }

  private static final Set<String> COORDINATE_NAMES = Set.of("pos", "posList", "coordinates");

  private static final Set<String> UNSUPPORTED_TOP =
      Set.of("CompositeSolid", "MultiSolid", "OrientableCurve", "OrientableSurface");

  private static final Set<String> UNSUPPORTED_SEGMENT =
      Set.of(
          "GeodesicString",
          "Geodesic",
          "CubicSpline",
          "BSpline",
          "Bezier",
          "ArcByCenterPoint",
          "CircleByCenterPoint",
          "ArcByBulge",
          "ArcStringByBulge",
          "OffsetCurve",
          "Clothoid");

  private static final Set<String> UNSUPPORTED_PATCH =
      Set.of("Triangle", "Rectangle", "TriangulatedSurface", "Tin", "Cone", "Cylinder", "Sphere");

  static class Frame {
    Kind kind;
    String elementName;
    Optional<EpsgCrs> crs = Optional.empty();
    OptionalInt srsDimension = OptionalInt.empty();

    double[] coords;
    StringBuilder textBuffer = new StringBuilder();
    final List<Geometry<?>> children = new ArrayList<>();
    boolean sawInterior; // for Solid: detect inner shell
  }

  private final Deque<Frame> stack = new ArrayDeque<>();
  private final Map<String, EpsgCrs> srsNameMappings;
  private final Set<String> verticalSrsNames;
  private boolean waitingForInput = false;
  private Geometry<?> result;

  /**
   * The verbatim {@code srsName} attribute of the outermost geometry element of the current decode,
   * or {@code null} when none was present. Unlike the resolved {@link EpsgCrs} this distinguishes
   * application-profile forms that map to the same EPSG code (e.g. the AdV realizations {@code
   * urn:adv:crs:DE_DHDN_3GK3_HE100} and {@code …HE120}).
   */
  private String rawSrsName;

  /**
   * Set when {@link #rawSrsName} is one of {@link #verticalSrsNames}: the "geometry" is a 1D
   * position (a single number in {@code gml:pos}, e.g. a height in a German height reference
   * system), which has no representation in the geometry model. The coordinate text is captured
   * verbatim in {@link #verticalValue} and no {@link Geometry} is built; {@code decode} returns
   * empty with {@code waitingForInput == false} once the subtree is consumed.
   */
  private boolean verticalMode;

  private String verticalValue;

  public GeometryDecoderGml() {
    this(Map.of());
  }

  /**
   * @param srsNameMappings reverse-mapping from {@code srsName} URI/URN forms to {@link EpsgCrs};
   *     consulted before the built-in EPSG / OGC URN parsers and intended to resolve
   *     application-profile forms (e.g. ALKIS NAS uses {@code urn:adv:crs:DE_DHDN_3GK2_NW101}) that
   *     the built-in parsers cannot handle.
   */
  public GeometryDecoderGml(Map<String, EpsgCrs> srsNameMappings) {
    this(srsNameMappings, Set.of());
  }

  /**
   * @param verticalSrsNames {@code srsName} URI/URN forms of 1D (vertical) reference systems; a
   *     geometry whose outermost element carries one of these is captured as a scalar value (see
   *     {@link #getVerticalValue()}) instead of being decoded into a {@link Geometry}.
   */
  public GeometryDecoderGml(Map<String, EpsgCrs> srsNameMappings, Set<String> verticalSrsNames) {
    this.srsNameMappings = srsNameMappings == null ? Map.of() : srsNameMappings;
    this.verticalSrsNames = verticalSrsNames == null ? Set.of() : verticalSrsNames;
  }

  /** The verbatim srsName of the outermost geometry element of the last completed decode. */
  public Optional<String> getRawSrsName() {
    return Optional.ofNullable(rawSrsName);
  }

  /**
   * The verbatim coordinate text of a 1D position, present when the last decode consumed a geometry
   * in a vertical reference system (see {@link #GeometryDecoderGml(Map, Set)}); in that case {@code
   * decode} returned empty although no further input is needed.
   */
  public Optional<String> getVerticalValue() {
    return Optional.ofNullable(verticalValue);
  }

  public Optional<Geometry<?>> decode(
      AsyncXMLStreamReader<AsyncByteArrayFeeder> parser,
      Optional<EpsgCrs> crs,
      OptionalInt srsDimension)
      throws XMLStreamException, IOException {
    // fresh decode of a new geometry property — the 4-arg overload is also used to continue a
    // paused decode and must not clear the per-geometry state
    this.rawSrsName = null;
    this.verticalMode = false;
    this.verticalValue = null;
    return decode(parser, crs, srsDimension, false);
  }

  public Optional<Geometry<?>> decode(
      AsyncXMLStreamReader<AsyncByteArrayFeeder> parser,
      Optional<EpsgCrs> defaultCrs,
      OptionalInt defaultSrsDimension,
      boolean useCurrentEvent)
      throws XMLStreamException, IOException {
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
          {
            String localName = parser.getLocalName();
            boolean isCoordElement =
                handleStart(parser, localName, defaultCrs, defaultSrsDimension);
            if (isCoordElement && !readCoordinateText(parser)) {
              return Optional.empty();
            }
            break;
          }

        case XMLStreamConstants.END_ELEMENT:
          handleEnd(parser.getLocalName());
          if (result != null && stack.isEmpty()) {
            Geometry<?> r = result;
            result = null;
            waitingForInput = false;
            return Optional.of(r);
          }
          if (verticalMode && stack.isEmpty()) {
            // 1D position fully consumed — no Geometry to return; the caller reads the captured
            // scalar via getVerticalValue()
            waitingForInput = false;
            return Optional.empty();
          }
          break;

        default:
          break;
      }
    }
    throw new IOException("Unexpected end of XML stream, no complete geometry found.");
  }

  public Optional<Geometry<?>> continueDecoding(
      AsyncXMLStreamReader<AsyncByteArrayFeeder> parser,
      Optional<EpsgCrs> defaultCrs,
      OptionalInt srsDimension,
      String localName,
      String bufferedText)
      throws XMLStreamException, IOException {
    if (COORDINATE_NAMES.contains(localName) && !stack.isEmpty()) {
      Frame top = stack.peek();
      if (top.kind == coordinateKind(localName)) {
        if (bufferedText != null) {
          top.textBuffer.append(bufferedText);
        }
        if (verticalMode) {
          this.verticalValue = top.textBuffer.toString().trim();
          stack.pop();
          return decode(parser, defaultCrs, srsDimension, false);
        }
        finalizeCoordinates(top);
        stack.pop();
        applyCoordinates(top);
        return decode(parser, defaultCrs, srsDimension, false);
      }
    }
    return decode(parser, defaultCrs, srsDimension, true);
  }

  public boolean isWaitingForInput() {
    return waitingForInput;
  }

  private static Kind coordinateKind(String localName) {
    return switch (localName) {
      case "pos" -> Kind.POS;
      case "posList" -> Kind.POS_LIST;
      case "coordinates" -> Kind.COORDINATES;
      default -> null;
    };
  }

  /** Returns true when the started element is a coordinate text element. */
  private boolean handleStart(
      AsyncXMLStreamReader<AsyncByteArrayFeeder> parser,
      String localName,
      Optional<EpsgCrs> defaultCrs,
      OptionalInt defaultSrsDimension)
      throws IOException {

    if (UNSUPPORTED_TOP.contains(localName)) {
      throw new IOException("Unsupported GML geometry type: " + localName);
    }
    if (UNSUPPORTED_PATCH.contains(localName)) {
      throw new IOException("Unsupported GML surface patch: " + localName);
    }
    if (UNSUPPORTED_SEGMENT.contains(localName)) {
      throw new IOException("Unsupported GML curve segment: " + localName);
    }

    if (COORDINATE_NAMES.contains(localName)) {
      Frame f = new Frame();
      f.kind = coordinateKind(localName);
      f.elementName = localName;
      stack.push(f);
      return true;
    }

    Kind kind = classify(localName);
    if (kind == null) {
      if (stack.isEmpty()) {
        throw new IOException("Unsupported GML geometry type: " + localName);
      }
      Frame f = new Frame();
      f.kind = Kind.UNKNOWN;
      f.elementName = localName;
      stack.push(f);
      return false;
    }

    // Reject Solid with inner shell as soon as we see <interior> directly under <Solid>
    if (kind == Kind.INTERIOR || kind == Kind.INNER_BOUNDARY_IS) {
      Frame parent = stack.peek();
      if (parent != null && parent.kind == Kind.SOLID) {
        throw new IOException("Solid with inner shells is not supported.");
      }
    }

    Frame f = new Frame();
    f.kind = kind;
    f.elementName = localName;

    if (isObject(kind)) {
      if (stack.isEmpty()) {
        // outermost geometry element of this decode — keep the verbatim srsName for callers that
        // need the original form, and detect 1D positions in vertical reference systems
        this.rawSrsName = parser.getAttributeValue(null, "srsName");
        this.verticalMode = rawSrsName != null && verticalSrsNames.contains(rawSrsName);
      }
      Optional<EpsgCrs> explicitCrs = parseSrsName(parser, srsNameMappings);
      f.crs = explicitCrs.or(() -> defaultCrs).or(this::inheritedCrs);
      OptionalInt dim = parseSrsDimension(parser);
      if (dim.isEmpty()) {
        dim = defaultSrsDimension.isPresent() ? defaultSrsDimension : inheritedSrsDimension();
      }
      f.srsDimension = dim;
    }

    stack.push(f);
    return false;
  }

  private void handleEnd(String localName) throws IOException {
    if (stack.isEmpty()) {
      throw new IllegalStateException("Unbalanced end element: " + localName);
    }
    Frame top = stack.peek();

    // tolerate stray closes that don't match our top frame (well-formed XML guarantees match)
    if (!localName.equals(top.elementName)) {
      return;
    }

    stack.pop();

    if (top.kind == Kind.UNKNOWN) {
      return;
    }

    if (verticalMode) {
      // a 1D position builds no Geometry; the scalar was captured in verticalValue
      return;
    }

    Geometry<?> built = buildGeometry(top);
    if (built == null) {
      // transparent wrapper — nothing to contribute
      return;
    }

    if (stack.isEmpty()) {
      result = built;
      return;
    }

    contributeToConsumer(built);
  }

  /** Adds {@code geom} to the nearest non-transparent ancestor in the stack. */
  private void contributeToConsumer(Geometry<?> geom) {
    Iterator<Frame> it = stack.iterator();
    while (it.hasNext()) {
      Frame anc = it.next();
      if (isTransparent(anc.kind)) {
        continue;
      }
      anc.children.add(geom);
      return;
    }
    // no consumer found — geometry becomes the root result
    result = geom;
  }

  private static boolean isTransparent(Kind kind) {
    return switch (kind) {
      case POINT_MEMBER,
          CURVE_MEMBER,
          LINE_STRING_MEMBER,
          POLYGON_MEMBER,
          SURFACE_MEMBER,
          GEOMETRY_MEMBER,
          POINT_MEMBERS,
          CURVE_MEMBERS,
          POLYGON_MEMBERS,
          SURFACE_MEMBERS,
          EXTERIOR,
          INTERIOR,
          OUTER_BOUNDARY_IS,
          INNER_BOUNDARY_IS,
          SHELL,
          SEGMENTS,
          PATCHES,
          UNKNOWN ->
          true;
      default -> false;
    };
  }

  /**
   * Frames that produce a geometry value; they capture srsName/srsDimension at start and inherit
   * from ancestors so that leaf primitives carry the same CRS as their enclosing object. Includes
   * rings, surface patches, and curve segments — none of those typically carry a srsName attribute
   * themselves, but they must still inherit one for the built primitive.
   */
  private static boolean isObject(Kind kind) {
    return switch (kind) {
      case POINT,
          LINE_STRING,
          POLYGON,
          CURVE,
          COMPOSITE_CURVE,
          SURFACE,
          COMPOSITE_SURFACE,
          POLYHEDRAL_SURFACE,
          SOLID,
          MULTI_POINT,
          MULTI_LINE_STRING,
          MULTI_CURVE,
          MULTI_POLYGON,
          MULTI_SURFACE,
          MULTI_GEOMETRY,
          LINEAR_RING,
          RING,
          POLYGON_PATCH,
          LINE_STRING_SEGMENT,
          ARC,
          ARC_STRING,
          CIRCLE ->
          true;
      default -> false;
    };
  }

  private static Kind classify(String localName) {
    return switch (localName) {
      case "Point" -> Kind.POINT;
      case "LineString" -> Kind.LINE_STRING;
      case "Polygon" -> Kind.POLYGON;
      case "Curve" -> Kind.CURVE;
      case "CompositeCurve" -> Kind.COMPOSITE_CURVE;
      case "Surface" -> Kind.SURFACE;
      case "CompositeSurface" -> Kind.COMPOSITE_SURFACE;
      case "PolyhedralSurface" -> Kind.POLYHEDRAL_SURFACE;
      case "Solid" -> Kind.SOLID;
      case "MultiPoint" -> Kind.MULTI_POINT;
      case "MultiLineString" -> Kind.MULTI_LINE_STRING;
      case "MultiCurve" -> Kind.MULTI_CURVE;
      case "MultiPolygon" -> Kind.MULTI_POLYGON;
      case "MultiSurface" -> Kind.MULTI_SURFACE;
      case "MultiGeometry" -> Kind.MULTI_GEOMETRY;
      case "pointMember" -> Kind.POINT_MEMBER;
      case "curveMember" -> Kind.CURVE_MEMBER;
      case "lineStringMember" -> Kind.LINE_STRING_MEMBER;
      case "polygonMember" -> Kind.POLYGON_MEMBER;
      case "surfaceMember" -> Kind.SURFACE_MEMBER;
      case "geometryMember" -> Kind.GEOMETRY_MEMBER;
      case "pointMembers" -> Kind.POINT_MEMBERS;
      case "curveMembers" -> Kind.CURVE_MEMBERS;
      case "polygonMembers" -> Kind.POLYGON_MEMBERS;
      case "surfaceMembers" -> Kind.SURFACE_MEMBERS;
      case "exterior" -> Kind.EXTERIOR;
      case "interior" -> Kind.INTERIOR;
      case "outerBoundaryIs" -> Kind.OUTER_BOUNDARY_IS;
      case "innerBoundaryIs" -> Kind.INNER_BOUNDARY_IS;
      case "Shell" -> Kind.SHELL;
      case "segments" -> Kind.SEGMENTS;
      case "patches" -> Kind.PATCHES;
      case "LinearRing" -> Kind.LINEAR_RING;
      case "Ring" -> Kind.RING;
      case "PolygonPatch" -> Kind.POLYGON_PATCH;
      case "LineStringSegment" -> Kind.LINE_STRING_SEGMENT;
      case "Arc" -> Kind.ARC;
      case "ArcString" -> Kind.ARC_STRING;
      case "Circle" -> Kind.CIRCLE;
      default -> null;
    };
  }

  private Optional<EpsgCrs> inheritedCrs() {
    for (Frame f : stack) {
      if (f.crs.isPresent()) {
        return f.crs;
      }
    }
    return Optional.empty();
  }

  private OptionalInt inheritedSrsDimension() {
    for (Frame f : stack) {
      if (f.srsDimension.isPresent()) {
        return f.srsDimension;
      }
    }
    return OptionalInt.empty();
  }

  private static Optional<EpsgCrs> parseSrsName(
      AsyncXMLStreamReader<AsyncByteArrayFeeder> parser, Map<String, EpsgCrs> srsNameMappings) {
    String srsName = parser.getAttributeValue(null, "srsName");
    if (srsName == null || srsName.isEmpty()) {
      return Optional.empty();
    }
    EpsgCrs mapped = srsNameMappings.get(srsName);
    if (mapped != null) {
      return Optional.of(mapped);
    }
    if (srsName.startsWith("urn:ogc:def:crs:EPSG::")) {
      try {
        return Optional.of(
            EpsgCrs.of(Integer.parseInt(srsName.substring(srsName.lastIndexOf(':') + 1))));
      } catch (Exception e) {
        // fall through
      }
    } else if (srsName.startsWith("http://www.opengis.net/def/crs/EPSG/0/")) {
      try {
        return Optional.of(
            EpsgCrs.of(Integer.parseInt(srsName.substring(srsName.lastIndexOf('/') + 1))));
      } catch (Exception e) {
        // fall through
      }
    } else if ("http://www.opengis.net/def/crs/OGC/0/CRS84".equals(srsName)
        || "http://www.opengis.net/def/crs/OGC/1.3/CRS84".equals(srsName)
        || "urn:ogc:def:crs:OGC:1.3:CRS84".equals(srsName)) {
      return Optional.of(OgcCrs.CRS84);
    } else if ("http://www.opengis.net/def/crs/OGC/0/CRS84h".equals(srsName)
        || "urn:ogc:def:crs:OGC::CRS84h".equals(srsName)) {
      return Optional.of(OgcCrs.CRS84h);
    }
    return Optional.empty();
  }

  private static OptionalInt parseSrsDimension(AsyncXMLStreamReader<AsyncByteArrayFeeder> parser) {
    String dim = parser.getAttributeValue(null, "srsDimension");
    if (dim != null) {
      try {
        return OptionalInt.of(Integer.parseInt(dim));
      } catch (NumberFormatException e) {
        // fall through
      }
    }
    return OptionalInt.empty();
  }

  private boolean readCoordinateText(AsyncXMLStreamReader<AsyncByteArrayFeeder> parser)
      throws XMLStreamException {
    Frame coordFrame = stack.peek();
    while (parser.hasNext()) {
      int event = parser.next();
      switch (event) {
        case AsyncXMLStreamReader.EVENT_INCOMPLETE:
          waitingForInput = true;
          return false;
        case XMLStreamConstants.CHARACTERS:
          coordFrame.textBuffer.append(parser.getText());
          break;
        case XMLStreamConstants.END_ELEMENT:
          if (coordFrame.elementName.equals(parser.getLocalName())) {
            if (verticalMode) {
              // a 1D position is captured verbatim — a single number is not a valid coordinate
              // tuple and must not reach the coordinate parser
              this.verticalValue = coordFrame.textBuffer.toString().trim();
              stack.pop();
              return true;
            }
            finalizeCoordinates(coordFrame);
            stack.pop();
            applyCoordinates(coordFrame);
            return true;
          }
          break;
        default:
          break;
      }
    }
    waitingForInput = true;
    return false;
  }

  private static void finalizeCoordinates(Frame coordFrame) {
    String text = coordFrame.textBuffer.toString().trim();
    coordFrame.coords = text.isEmpty() ? new double[0] : parseDoubles(text);
  }

  private void applyCoordinates(Frame coordFrame) {
    if (stack.isEmpty()) {
      return;
    }
    Frame parent = stack.peek();
    if (parent.coords == null) {
      parent.coords = coordFrame.coords;
    } else {
      double[] a = parent.coords;
      double[] b = coordFrame.coords;
      double[] c = new double[a.length + b.length];
      System.arraycopy(a, 0, c, 0, a.length);
      System.arraycopy(b, 0, c, a.length, b.length);
      parent.coords = c;
    }
  }

  private static Axes axesOf(Frame f) {
    if (f.srsDimension.isPresent()) {
      return f.srsDimension.getAsInt() >= 3 ? Axes.XYZ : Axes.XY;
    }
    return Axes.XY;
  }

  // -- builders --------------------------------------------------------------

  private Geometry<?> buildGeometry(Frame f) throws IOException {
    return switch (f.kind) {
      case POINT -> buildPoint(f);
      case LINE_STRING -> buildSinglePosList(f, false);
      case CURVE -> buildCurve(f);
      case COMPOSITE_CURVE -> buildCompositeCurve(f);
      case POLYGON -> buildPolygon(f);
      case SURFACE -> buildSurface(f);
      case COMPOSITE_SURFACE -> buildCompositeSurface(f);
      case POLYHEDRAL_SURFACE -> buildPolyhedralSurface(f, false);
      case SOLID -> buildSolid(f);
      case MULTI_POINT -> buildMultiPoint(f);
      case MULTI_LINE_STRING -> buildMultiLineString(f);
      case MULTI_CURVE -> buildMultiCurve(f);
      case MULTI_POLYGON -> buildMultiPolygon(f);
      case MULTI_SURFACE -> buildMultiSurface(f);
      case MULTI_GEOMETRY -> buildMultiGeometry(f);
      case LINEAR_RING -> buildSinglePosList(f, false);
      case RING -> buildRing(f);
      case POLYGON_PATCH -> buildPolygon(f);
      case LINE_STRING_SEGMENT -> buildSinglePosList(f, false);
      case ARC, ARC_STRING -> buildSinglePosList(f, true);
      case CIRCLE -> buildCircle(f);
      default -> null;
    };
  }

  private Point buildPoint(Frame f) throws IOException {
    double[] c = f.coords != null ? f.coords : new double[0];
    if (c.length == 0) {
      throw new IOException("Empty <gml:Point>.");
    }
    Axes axes = c.length >= 3 ? Axes.XYZ : Axes.XY;
    return Point.of(Position.of(axes, c), f.crs);
  }

  /**
   * A {@code <gml:Circle>} is 3 points defining a full circle. Internally we store it as a
   * 5-position closed CIRCULARSTRING — {@code (P1, P2, P3, antipode(P2), P1)} — so it satisfies
   * ring-closure validation and round-trips through WKT/WKB into PostGIS as a full circle. The
   * original 3-point form is recovered by {@link GeometryEncoderGml} when emitting GML.
   */
  private Geometry<?> buildCircle(Frame f) throws IOException {
    double[] c = f.coords != null ? f.coords : new double[0];
    if (c.length == 0) {
      throw new IOException("Empty <gml:Circle>.");
    }
    Axes axes = axesOf(f);
    if (axes != Axes.XY) {
      // 3D circles would need the plane of the circle resolved in 3-space; no current data
      // requires this. Fall back to the linear 3-point form (which downstream ring-closure
      // validation will reject loudly).
      return buildSinglePosList(f, true);
    }
    if (c.length != 6) {
      throw new IOException(
          "<gml:Circle> requires exactly 3 XY positions, got " + (c.length / 2) + ".");
    }
    double[] expanded;
    try {
      expanded = Circles.expandCircleToClosed(c);
    } catch (IllegalArgumentException colinear) {
      throw new IOException("Invalid <gml:Circle>: " + colinear.getMessage());
    }
    return CircularString.of(PositionList.of(axes, expanded), f.crs);
  }

  private Geometry<?> buildSinglePosList(Frame f, boolean curved) throws IOException {
    double[] c = f.coords != null ? f.coords : new double[0];
    if (c.length == 0) {
      throw new IOException("Empty <gml:" + f.elementName + ">.");
    }
    Axes axes = axesOf(f);
    // If srsDimension wasn't declared explicitly anywhere in scope, heuristically pick 3D when
    // total coordinate count is divisible by 3 but not by 2. Otherwise default 2D.
    if (f.srsDimension.isEmpty()) {
      if (c.length % 2 != 0 && c.length % 3 == 0) {
        axes = Axes.XYZ;
      }
    }
    PositionList pl = PositionList.of(axes, c);
    return curved ? CircularString.of(pl, f.crs) : LineString.of(pl, f.crs);
  }

  /**
   * Each curveMember of a Ring carries a primitive Curve. Primitives are preserved — a Ring with
   * multiple curveMembers becomes a CompoundCurve even if every member is linear, since merging
   * separate primitive Curves into one LineString would erase the input's primitive structure.
   */
  private Curve<?> buildRing(Frame f) throws IOException {
    if (f.children.isEmpty()) {
      throw new IOException("Empty <gml:Ring>.");
    }
    if (f.children.size() == 1) {
      Geometry<?> only = f.children.get(0);
      if (only instanceof LineString ls) {
        return ls;
      }
      if (only instanceof CircularString cs) {
        return cs;
      }
      if (only instanceof CompoundCurve cc) {
        return cc;
      }
    }
    List<SingleCurve> segs = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof SingleCurve sc) {
        segs.add(sc);
      } else if (g instanceof CompoundCurve cc) {
        // CompoundCurve cannot nest in our model; flatten — the curveMember had heterogeneous
        // segments which already lost primitive identity at Curve build time.
        segs.addAll(cc.getValue());
      } else {
        throw new IOException(
            "Unsupported geometry inside <gml:Ring>: " + g.getClass().getSimpleName());
      }
    }
    return CompoundCurve.of(segs, f.crs);
  }

  private static Axes axesFromMembers(List<Geometry<?>> members) {
    for (Geometry<?> g : members) {
      if (!g.isEmpty()) {
        return g.getAxes();
      }
    }
    return Axes.XY;
  }

  /** Concatenate connected SingleCurve segments, dropping each segment's duplicated start. */
  private static double[] mergeConnectedSegments(List<Geometry<?>> segments, Axes axes) {
    int dim = axes.size();
    int total = 0;
    for (int i = 0; i < segments.size(); i++) {
      SingleCurve sc = (SingleCurve) segments.get(i);
      int n = sc.getValue().getNumPositions();
      total += i == 0 ? n : Math.max(0, n - 1);
    }
    double[] out = new double[total * dim];
    int pos = 0;
    for (int i = 0; i < segments.size(); i++) {
      SingleCurve sc = (SingleCurve) segments.get(i);
      double[] cc = sc.getValue().getCoordinates();
      int start = i == 0 ? 0 : dim;
      System.arraycopy(cc, start, out, pos, cc.length - start);
      pos += cc.length - start;
    }
    return out;
  }

  /**
   * Segments inside a single {@code <gml:Curve>} are parts of one primitive Curve, not separate
   * primitives — consecutive segments of the same type are merged into one primitive (LineString or
   * CircularString). Heterogeneous segments become a CompoundCurve.
   */
  private Geometry<?> buildCurve(Frame f) throws IOException {
    if (f.children.isEmpty()) {
      throw new IOException("Empty <gml:Curve>.");
    }
    if (f.children.size() == 1) {
      return f.children.get(0);
    }
    boolean allLinear = f.children.stream().allMatch(g -> g instanceof LineString);
    if (allLinear) {
      Axes axes = axesFromMembers(f.children);
      return LineString.of(PositionList.of(axes, mergeConnectedSegments(f.children, axes)), f.crs);
    }
    boolean allCircular = f.children.stream().allMatch(g -> g instanceof CircularString);
    if (allCircular) {
      Axes axes = axesFromMembers(f.children);
      return CircularString.of(
          PositionList.of(axes, mergeConnectedSegments(f.children, axes)), f.crs);
    }
    List<SingleCurve> sc = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof SingleCurve s) {
        sc.add(s);
      } else {
        throw new IOException(
            "Unsupported geometry inside <gml:Curve>: " + g.getClass().getSimpleName());
      }
    }
    return CompoundCurve.of(sc, f.crs);
  }

  private Geometry<?> buildCompositeCurve(Frame f) throws IOException {
    if (f.children.isEmpty()) {
      throw new IOException("Empty <gml:CompositeCurve>.");
    }
    List<SingleCurve> sc = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof SingleCurve s) {
        sc.add(s);
      } else if (g instanceof CompoundCurve cc) {
        sc.addAll(cc.getValue());
      } else {
        throw new IOException(
            "Unsupported geometry inside <gml:CompositeCurve>: " + g.getClass().getSimpleName());
      }
    }
    if (sc.size() == 1) {
      return sc.get(0);
    }
    return CompoundCurve.of(sc, f.crs);
  }

  private Geometry<?> buildPolygon(Frame f) throws IOException {
    if (f.children.isEmpty()) {
      throw new IOException("Empty <gml:" + f.elementName + ">.");
    }
    boolean allLinear = f.children.stream().allMatch(g -> g instanceof LineString);
    if (allLinear) {
      List<PositionList> rings = new ArrayList<>(f.children.size());
      for (Geometry<?> g : f.children) {
        rings.add(((LineString) g).getValue());
      }
      return Polygon.of(rings, f.crs);
    }
    List<Curve<?>> rings = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof Curve<?> c) {
        rings.add(c);
      } else {
        throw new IOException(
            "Unsupported ring geometry inside <gml:"
                + f.elementName
                + ">: "
                + g.getClass().getSimpleName());
      }
    }
    return CurvePolygon.of(rings, f.crs);
  }

  private Geometry<?> buildSurface(Frame f) throws IOException {
    if (f.children.isEmpty()) {
      throw new IOException("Empty <gml:Surface>.");
    }
    if (f.children.size() == 1) {
      return f.children.get(0);
    }
    List<Polygon> polys = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof Polygon p) {
        polys.add(p);
      } else {
        throw new IOException(
            "Multi-patch <gml:Surface> requires planar PolygonPatches; found "
                + g.getClass().getSimpleName());
      }
    }
    return PolyhedralSurface.of(polys, false, f.crs);
  }

  private Geometry<?> buildCompositeSurface(Frame f) throws IOException {
    if (f.children.isEmpty()) {
      throw new IOException("Empty <gml:CompositeSurface>.");
    }
    boolean allPlanar = f.children.stream().allMatch(g -> g instanceof Polygon);
    if (allPlanar) {
      List<Polygon> polys = new ArrayList<>();
      for (Geometry<?> g : f.children) {
        polys.add((Polygon) g);
      }
      return PolyhedralSurface.of(polys, false, f.crs);
    }
    List<Surface<?>> surfaces = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof Surface<?> s) {
        surfaces.add(s);
      } else {
        throw new IOException(
            "Unsupported geometry inside <gml:CompositeSurface>: " + g.getClass().getSimpleName());
      }
    }
    return MultiSurface.of(surfaces, f.crs);
  }

  private Geometry<?> buildPolyhedralSurface(Frame f, boolean closed) throws IOException {
    if (f.children.isEmpty()) {
      throw new IOException("Empty <gml:PolyhedralSurface>.");
    }
    List<Polygon> polys = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof Polygon p) {
        polys.add(p);
      } else {
        throw new IOException(
            "Non-planar patch in <gml:PolyhedralSurface>: " + g.getClass().getSimpleName());
      }
    }
    return PolyhedralSurface.of(polys, closed, f.crs);
  }

  private Geometry<?> buildSolid(Frame f) throws IOException {
    if (f.children.isEmpty()) {
      throw new IOException("Empty <gml:Solid>.");
    }
    List<Polygon> polys = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof Polygon p) {
        polys.add(p);
      } else {
        throw new IOException("Non-planar surface in <gml:Solid>: " + g.getClass().getSimpleName());
      }
    }
    return PolyhedralSurface.of(polys, true, f.crs);
  }

  private Geometry<?> buildMultiPoint(Frame f) throws IOException {
    List<Point> pts = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof Point p) {
        pts.add(p);
      } else {
        throw new IOException(
            "Unsupported member in <gml:MultiPoint>: " + g.getClass().getSimpleName());
      }
    }
    return MultiPoint.of(pts, f.crs);
  }

  private Geometry<?> buildMultiLineString(Frame f) throws IOException {
    List<LineString> ls = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof LineString l) {
        ls.add(l);
      } else {
        throw new IOException(
            "Unsupported member in <gml:MultiLineString>: " + g.getClass().getSimpleName());
      }
    }
    return MultiLineString.of(ls, f.crs);
  }

  private Geometry<?> buildMultiCurve(Frame f) throws IOException {
    boolean allLinear = f.children.stream().allMatch(g -> g instanceof LineString);
    if (allLinear) {
      List<LineString> ls = new ArrayList<>();
      for (Geometry<?> g : f.children) {
        ls.add((LineString) g);
      }
      return MultiLineString.of(ls, f.crs);
    }
    List<Curve<?>> cs = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof Curve<?> c) {
        cs.add(c);
      } else {
        throw new IOException(
            "Unsupported member in <gml:MultiCurve>: " + g.getClass().getSimpleName());
      }
    }
    return MultiCurve.of(cs, f.crs);
  }

  private Geometry<?> buildMultiPolygon(Frame f) throws IOException {
    List<Polygon> ps = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof Polygon p) {
        ps.add(p);
      } else {
        throw new IOException(
            "Unsupported member in <gml:MultiPolygon>: " + g.getClass().getSimpleName());
      }
    }
    return MultiPolygon.of(ps, f.crs);
  }

  private Geometry<?> buildMultiSurface(Frame f) throws IOException {
    if (f.children.stream().allMatch(g -> g instanceof Polygon)) {
      List<Polygon> ps = new ArrayList<>();
      for (Geometry<?> g : f.children) {
        ps.add((Polygon) g);
      }
      return MultiPolygon.of(ps, f.crs);
    }
    List<Surface<?>> ss = new ArrayList<>();
    for (Geometry<?> g : f.children) {
      if (g instanceof Surface<?> s) {
        ss.add(s);
      } else {
        throw new IOException(
            "Unsupported member in <gml:MultiSurface>: " + g.getClass().getSimpleName());
      }
    }
    return MultiSurface.of(ss, f.crs);
  }

  private Geometry<?> buildMultiGeometry(Frame f) {
    return GeometryCollection.of(ImmutableList.copyOf(f.children), f.crs);
  }

  private static double[] parseDoubles(String text) {
    String[] parts = text.trim().split("[ \n\r\t,]+");
    double[] coords = new double[parts.length];
    for (int i = 0; i < parts.length; i++) {
      coords[i] = Double.parseDouble(parts[i]);
    }
    return coords;
  }
}
