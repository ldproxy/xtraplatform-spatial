/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.geometries.domain.transcode.wktwkb;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.geometries.domain.Axes;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.geometries.domain.Position;
import de.ii.xtraplatform.geometries.domain.PositionList;
import de.ii.xtraplatform.geometries.domain.transcode.AbstractGeometryDecoder;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class GeometryDecoderWkt extends AbstractGeometryDecoder {

  private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();
  private static final Splitter BLANK_SPLITTER = Splitter.on(' ').omitEmptyStrings().trimResults();
  private static final String EMPTY = "EMPTY",
      ZM = "ZM",
      Z = "Z",
      M = "M",
      L_PAREN = "(",
      R_PAREN = ")",
      COMMA = ",";

  public Geometry<?> decode(String wkt) throws IOException {
    return decode(wkt, Optional.empty());
  }

  public Geometry<?> decode(String wkt, Optional<EpsgCrs> crs) throws IOException {
    return decode(getStreamTokenizer(normalizeWkt(wkt)), crs, null, Set.of(), null);
  }

  public Geometry<?> decodeSimpleFeature(String wkt, Optional<EpsgCrs> crs) throws IOException {
    return decode(
        getStreamTokenizer(normalizeWkt(wkt)),
        crs,
        null,
        Set.of(
            GeometryType.POINT,
            GeometryType.LINE_STRING,
            GeometryType.POLYGON,
            GeometryType.MULTI_POINT,
            GeometryType.MULTI_LINE_STRING,
            GeometryType.MULTI_POLYGON,
            GeometryType.GEOMETRY_COLLECTION),
        null);
  }

  public Geometry<?> decode(
      StreamTokenizer tokenizer,
      Optional<EpsgCrs> crs,
      GeometryType implicitType,
      Set<GeometryType> allowedTypes,
      Axes allowedAxes)
      throws IOException {
    GeometryType type;
    Axes axes = Axes.XY;
    boolean isEmpty = false;

    type = null;
    String t = getNextToken(tokenizer);
    if (implicitType != null) {
      // check if the type is implicit (that is, the next token is "EMPTY" or "(")
      if (t.equals(EMPTY)) {
        type = implicitType;
        isEmpty = true;
      } else if (t.equals(L_PAREN)) {
        type = implicitType;
        tokenizer.pushBack();
      }
    } else {
      if (t.endsWith(EMPTY)) {
        isEmpty = true;
        t = removeTail(t, EMPTY.length());
      }
      if (t.endsWith(ZM)) {
        axes = Axes.XYZM;
        t = removeTail(t, ZM.length());
      } else if (t.endsWith(Z)) {
        axes = Axes.XYZ;
        t = removeTail(t, Z.length());
      } else if (t.endsWith(M)) {
        axes = Axes.XYM;
        t = removeTail(t, M.length());
      }
    }

    if (type == null) {
      try {
        type = WktWkbGeometryType.valueOf(t).toGeometryType();
      } catch (IllegalArgumentException e) {
        throw new IllegalStateException("Unknown geometry type: " + t);
      }
    }

    if (!allowedTypes.isEmpty() && !allowedTypes.contains(type)) {
      throw new IllegalStateException(
          "Unexpected geometry type " + type + ". Allowed: " + allowedTypes);
    }
    if (allowedAxes != null && !allowedAxes.equals(axes)) {
      throw new IllegalStateException(
          "Geometry axes " + axes + " do not match expected axes " + allowedAxes);
    }

    if (isEmpty) {
      return empty(type, axes);
    }

    return switch (type) {
      case POINT -> point(readPosition(tokenizer, crs, axes), crs);
      case MULTI_POINT -> multiPoint(readPositions(tokenizer, axes), crs);
      case LINE_STRING -> lineString(readPositionList(tokenizer, axes), crs);
      case CIRCULAR_STRING -> circularString(readPositionList(tokenizer, axes), crs);
      case MULTI_LINE_STRING -> multiLineString(readListOfPositionList(tokenizer, axes), crs);
      case POLYGON -> polygon(readListOfPositionList(tokenizer, axes), crs);
      case MULTI_POLYGON -> multiPolygon(readListOfListOfPositionList(tokenizer, axes), crs);
      case POLYHEDRAL_SURFACE -> polyhedralSurface(
          readListOfListOfPositionList(tokenizer, axes), crs);
      case COMPOUND_CURVE -> compoundCurve(
          readListOf(
              tokenizer,
              crs,
              axes,
              GeometryType.LINE_STRING,
              Set.of(GeometryType.LINE_STRING, GeometryType.CIRCULAR_STRING)),
          crs);
      case CURVE_POLYGON -> curvePolygon(
          readListOf(
              tokenizer,
              crs,
              axes,
              GeometryType.LINE_STRING,
              Set.of(
                  GeometryType.LINE_STRING,
                  GeometryType.CIRCULAR_STRING,
                  GeometryType.COMPOUND_CURVE)),
          crs);
      case MULTI_CURVE -> multiCurve(
          readListOf(
              tokenizer,
              crs,
              axes,
              GeometryType.LINE_STRING,
              Set.of(
                  GeometryType.LINE_STRING,
                  GeometryType.CIRCULAR_STRING,
                  GeometryType.COMPOUND_CURVE)),
          crs);
      case MULTI_SURFACE -> multiSurface(
          readListOf(
              tokenizer,
              crs,
              axes,
              GeometryType.POLYGON,
              Set.of(GeometryType.POLYGON, GeometryType.CURVE_POLYGON)),
          crs);
      case GEOMETRY_COLLECTION -> geometryCollection(
          readListOf(
              tokenizer,
              crs,
              axes,
              null,
              Set.of(
                  GeometryType.POINT,
                  GeometryType.LINE_STRING,
                  GeometryType.POLYGON,
                  GeometryType.MULTI_POINT,
                  GeometryType.MULTI_LINE_STRING,
                  GeometryType.MULTI_POLYGON,
                  GeometryType.GEOMETRY_COLLECTION)),
          crs);
      default -> throw new IllegalStateException("Unsupported geometry type: " + type);
    };
  }

  private Position readPosition(StreamTokenizer tokenizer, Optional<EpsgCrs> crs, Axes axes)
      throws IOException {
    expectToken(tokenizer, L_PAREN);
    Position pos =
        Position.of(
            axes,
            BLANK_SPLITTER
                .splitToStream(getNextToken(tokenizer))
                .mapToDouble(Double::parseDouble)
                .toArray());
    expectToken(tokenizer, R_PAREN);
    return pos;
  }

  private List<Position> readPositions(StreamTokenizer tokenizer, Axes axes) throws IOException {
    ImmutableList.Builder<Position> builder = ImmutableList.builder();
    expectToken(tokenizer, L_PAREN);
    String next = COMMA;
    while (COMMA.equals(next)) {
      next = getNextToken(tokenizer);
      if (!EMPTY.equals(next)) {
        if (L_PAREN.equals(next)) {
          Position pos =
              Position.of(
                  axes,
                  BLANK_SPLITTER
                      .splitToStream(getNextToken(tokenizer))
                      .mapToDouble(Double::parseDouble)
                      .toArray());
          builder.add(pos);
          expectToken(tokenizer, R_PAREN);
        } else {
          builder.add(
              Position.of(
                  axes,
                  BLANK_SPLITTER.splitToStream(next).mapToDouble(Double::parseDouble).toArray()));
        }
      }
      next = getCommaOrCloser(tokenizer);
    }
    return builder.build();
  }

  private PositionList readPositionList(StreamTokenizer tokenizer, Axes axes) throws IOException {
    StringBuilder builder = new StringBuilder();
    expectToken(tokenizer, L_PAREN);
    String next = COMMA;
    boolean first = true;
    while (COMMA.equals(next)) {
      next = getNextToken(tokenizer);
      if (!EMPTY.equals(next)) {
        if (!first) {
          builder.append(COMMA);
        } else {
          first = false;
        }
        builder.append(next);
      }
      next = getCommaOrCloser(tokenizer);
    }
    return PositionList.of(
        axes,
        COMMA_SPLITTER
            .splitToStream(builder.toString())
            .map(pos -> BLANK_SPLITTER.splitToStream(pos).mapToDouble(Double::parseDouble))
            .flatMapToDouble(coordinate -> coordinate)
            .toArray());
  }

  private List<PositionList> readListOfPositionList(StreamTokenizer tokenizer, Axes axes)
      throws IOException {
    ImmutableList.Builder<PositionList> builder = ImmutableList.builder();
    expectToken(tokenizer, L_PAREN);
    String next = COMMA;
    while (COMMA.equals(next)) {
      builder.add(readPositionList(tokenizer, axes));
      next = getCommaOrCloser(tokenizer);
    }
    return builder.build();
  }

  private List<List<PositionList>> readListOfListOfPositionList(
      StreamTokenizer tokenizer, Axes axes) throws IOException {
    ImmutableList.Builder<List<PositionList>> builder = ImmutableList.builder();
    expectToken(tokenizer, L_PAREN);
    String next = COMMA;
    while (COMMA.equals(next)) {
      List<PositionList> posListList = readListOfPositionList(tokenizer, axes);
      if (!posListList.isEmpty()) {
        builder.add(posListList);
      }
      next = getCommaOrCloser(tokenizer);
    }
    return builder.build();
  }

  private List<Geometry<?>> readListOf(
      StreamTokenizer tokenizer,
      Optional<EpsgCrs> crs,
      Axes axes,
      GeometryType defaultType,
      Set<GeometryType> allowedTypes)
      throws IOException {
    ImmutableList.Builder<Geometry<?>> builder = ImmutableList.builder();
    expectToken(tokenizer, L_PAREN);
    String next = COMMA;
    while (COMMA.equals(next)) {
      next = getNextToken(tokenizer);
      if (!EMPTY.equals(next)) {
        tokenizer.pushBack();
        builder.add(decode(tokenizer, crs, defaultType, allowedTypes, axes));
      }
      next = getCommaOrCloser(tokenizer);
    }
    return builder.build();
  }

  private static String removeTail(String type, int len) {
    return type.substring(0, Math.max(type.length() - len, 0)).trim();
  }

  private static StreamTokenizer getStreamTokenizer(String text) {
    StreamTokenizer t = new StreamTokenizer(new StringReader(text));
    t.resetSyntax();
    t.wordChars('a', 'z');
    t.wordChars('A', 'Z');
    t.wordChars('0', '9');
    t.wordChars('-', '-');
    t.wordChars('+', '+');
    t.wordChars('.', '.');
    t.wordChars(' ', ' ');
    return t;
  }

  private static String normalizeWkt(String wkt) {
    return wkt.replaceAll("\\s+", " ")
        .replace(") ", ")")
        .replace("( ", "(")
        .replace(" )", ")")
        .replace(" (", "(")
        .replace(", ", ",")
        .replace(" ,", ",");
  }

  private String getNextToken(StreamTokenizer t) throws IOException {
    int type = t.nextToken();
    return switch (type) {
      case '(' -> L_PAREN;
      case ')' -> R_PAREN;
      case ',' -> COMMA;
      case StreamTokenizer.TT_WORD -> t.sval;
      default -> throw new IllegalStateException("Error parsing WKT geometry at: " + t.sval);
    };
  }

  private void expectToken(StreamTokenizer t, String expected) throws IOException {
    String next = getNextToken(t);
    if (!expected.equals(next))
      throw new IllegalStateException("Expected '" + expected + "', but found: " + next);
  }

  private String getCommaOrCloser(StreamTokenizer t) throws IOException {
    String next = getNextToken(t);
    if (COMMA.equals(next) || R_PAREN.equals(next)) return next;
    throw new IllegalStateException("Expected ',' or ')', but found: " + next);
  }
}
