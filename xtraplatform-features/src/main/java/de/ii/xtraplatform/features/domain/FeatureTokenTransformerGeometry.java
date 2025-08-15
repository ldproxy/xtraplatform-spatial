/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import de.ii.xtraplatform.geometries.domain.CompoundCurve;
import de.ii.xtraplatform.geometries.domain.Geometry;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.geometries.domain.LineString;
import de.ii.xtraplatform.geometries.domain.MultiLineString;
import de.ii.xtraplatform.geometries.domain.MultiPoint;
import de.ii.xtraplatform.geometries.domain.MultiPolygon;
import de.ii.xtraplatform.geometries.domain.Point;
import de.ii.xtraplatform.geometries.domain.Polygon;
import de.ii.xtraplatform.geometries.domain.PolyhedralSurface;
import de.ii.xtraplatform.geometries.domain.SingleCurve;
import java.util.List;

public class FeatureTokenTransformerGeometry extends FeatureTokenTransformer {

  public FeatureTokenTransformerGeometry() {}

  @Override
  public void onGeometry(ModifiableContext<FeatureSchema, SchemaMapping> context) {
    Geometry<?> geometry = context.geometry();
    if (geometry != null) {
      // Upgrade geometries to their true type, if a simpler type was used in the feature provider,
      // typically because the provider cannot represent all information. Also upgrade primitive
      // geometries to multi geometries, if the schema expects a multi geometry.
      if (geometry.getType() == GeometryType.POLYHEDRAL_SURFACE
          && !((PolyhedralSurface) geometry).isClosed()
          && context
              .schema()
              .flatMap(s -> s.getConstraints().flatMap(SchemaConstraints::getClosed))
              .orElse(false)) {
        context.setGeometry(
            PolyhedralSurface.of(
                ((PolyhedralSurface) geometry).getValue(), true, geometry.getCrs()));
      } else if (geometry.getType() == GeometryType.MULTI_POLYGON
          && context
              .schema()
              .flatMap(s -> s.getConstraints().flatMap(SchemaConstraints::getComposite))
              .orElse(false)
          && context
              .schema()
              .flatMap(s -> s.getConstraints().flatMap(SchemaConstraints::getClosed))
              .orElse(false)) {
        context.setGeometry(
            PolyhedralSurface.of(((MultiPolygon) geometry).getValue(), true, geometry.getCrs()));
      } else if (geometry.getType() == GeometryType.MULTI_LINE_STRING
          && context
              .schema()
              .flatMap(s -> s.getConstraints().flatMap(SchemaConstraints::getComposite))
              .orElse(false)) {
        context.setGeometry(
            CompoundCurve.of(
                ((MultiLineString) geometry)
                    .getValue().stream().map(SingleCurve.class::cast).toList(),
                geometry.getCrs()));
      } else if (geometry.getType() == GeometryType.POINT
          && context.schema().flatMap(SchemaBase::getGeometryType).orElse(GeometryType.ANY)
              == GeometryType.MULTI_POINT) {
        context.setGeometry(MultiPoint.of(List.of((Point) geometry)));
      } else if (geometry.getType() == GeometryType.LINE_STRING
          && context.schema().flatMap(SchemaBase::getGeometryType).orElse(GeometryType.ANY)
              == GeometryType.MULTI_LINE_STRING) {
        context.setGeometry(MultiLineString.of(List.of((LineString) geometry)));
      } else if (geometry.getType() == GeometryType.POLYGON
          && context.schema().flatMap(SchemaBase::getGeometryType).orElse(GeometryType.ANY)
              == GeometryType.MULTI_POLYGON) {
        context.setGeometry(MultiPolygon.of(List.of((Polygon) geometry)));
      }
    }

    getDownstream().onGeometry(context);
  }
}
