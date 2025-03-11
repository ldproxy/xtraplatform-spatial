/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.domain.FeatureEventHandler;
import de.ii.xtraplatform.features.domain.FeatureEventHandler.ModifiableContext;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.SchemaMapping;
import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;
import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

public class GeometryDecoderWkb {

  private final FeatureEventHandler<
          FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
      handler;
  private final ModifiableContext<FeatureSchema, SchemaMapping> context;
  private final WKBReader wkbReader;

  public GeometryDecoderWkb(
      FeatureEventHandler<
              FeatureSchema, SchemaMapping, ModifiableContext<FeatureSchema, SchemaMapping>>
          handler,
      ModifiableContext<FeatureSchema, SchemaMapping> context) {
    this.handler = handler;
    this.context = context;
    this.wkbReader = new WKBReader();
  }

  public void decode(byte[] wkb) throws IOException {
    try {
      Geometry geometry = wkbReader.read(wkb);
      SimpleFeatureGeometry geometryType = SimpleFeatureGeometryFromToWkb.fromGeometry(geometry);
      int dimension = geometry.getCoordinate().z != Double.NaN ? 3 : 2;

      if (!geometryType.isValid()) {
        return;
      }

      context.setGeometryType(geometryType);
      context.setGeometryDimension(dimension);
      handler.onObjectStart(context);
      context.setInGeometry(true);

      switch (geometryType) {
        case POINT:
          handlePoint(geometry);
          break;
        case MULTI_POINT:
          handleMultiPoint(geometry);
          break;
        case LINE_STRING:
          handleLineString(geometry);
          break;
        case MULTI_LINE_STRING:
          handleMultiLineString(geometry);
          break;
        case POLYGON:
          handlePolygon(geometry);
          break;
        case MULTI_POLYGON:
          handleMultiPolygon(geometry);
          break;
        case GEOMETRY_COLLECTION:
          break;
      }

      context.setInGeometry(false);
      handler.onObjectEnd(context);
      context.setGeometryType(Optional.empty());
      context.setGeometryDimension(OptionalInt.empty());
    } catch (Exception e) {
      throw new IOException("Failed to decode WKB", e);
    }
  }

  private void handlePoint(Geometry geometry) {
    context.setValueType(Type.STRING);
    context.setValue(geometry.toText());
    handler.onValue(context);
  }

  private void handleMultiPoint(Geometry geometry) {
    handler.onArrayStart(context);
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      handlePoint(geometry.getGeometryN(i));
    }
    handler.onArrayEnd(context);
  }

  private void handleLineString(Geometry geometry) {
    context.setValueType(Type.STRING);
    context.setValue(geometry.toText());
    handler.onValue(context);
  }

  private void handleMultiLineString(Geometry geometry) {
    handler.onArrayStart(context);
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      handleLineString(geometry.getGeometryN(i));
    }
    handler.onArrayEnd(context);
  }

  private void handlePolygon(Geometry geometry) {
    context.setValueType(Type.STRING);
    context.setValue(geometry.toText());
    handler.onValue(context);
  }

  private void handleMultiPolygon(Geometry geometry) {
    handler.onArrayStart(context);
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      handlePolygon(geometry.getGeometryN(i));
    }
    handler.onArrayEnd(context);
  }
}
