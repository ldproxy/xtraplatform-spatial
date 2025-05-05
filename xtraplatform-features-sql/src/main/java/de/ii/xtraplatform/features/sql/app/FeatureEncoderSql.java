/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.ii.xtraplatform.base.domain.util.Tuple;
import de.ii.xtraplatform.crs.domain.CrsTransformer;
import de.ii.xtraplatform.crs.domain.CrsTransformerFactory;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.MappingRule;
import de.ii.xtraplatform.features.domain.PropertyBase;
import de.ii.xtraplatform.features.domain.PropertyBase.Type;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import de.ii.xtraplatform.features.domain.pipeline.FeatureEventHandlerSimple.ModifiableContext;
import de.ii.xtraplatform.features.domain.pipeline.FeatureTokenEncoderBaseSimple;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn;
import de.ii.xtraplatform.features.sql.domain.SqlQueryColumn.Operation;
import de.ii.xtraplatform.features.sql.domain.SqlQueryMapping;
import de.ii.xtraplatform.features.sql.domain.SqlQuerySchema;
import de.ii.xtraplatform.geometries.domain.GeometryWithStringCoordinates;
import de.ii.xtraplatform.geometries.domain.ImmutableCoordinatesTransformer;
import de.ii.xtraplatform.geometries.domain.ImmutableCoordinatesTransformer.Builder;
import de.ii.xtraplatform.geometries.domain.ImmutableCoordinatesWriterWkt;
import de.ii.xtraplatform.geometries.domain.SimpleFeatureGeometry;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.immutables.value.Value;
import org.immutables.value.Value.Modifiable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureEncoderSql
    extends FeatureTokenEncoderBaseSimple<
        SqlQuerySchema,
        SqlQueryMapping,
        ModifiableContext<SqlQuerySchema, SqlQueryMapping>,
        FeatureDataSql> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureEncoderSql.class);

  private final SqlQueryMapping mapping;
  private final EpsgCrs nativeCrs;
  private final Optional<CrsTransformer> crsTransformer;
  private final Optional<String> nullValue;
  private final ObjectMapper jsonMapper;

  private ModifiableFeatureDataSql currentFeature;
  private GeometryWithStringCoordinates<?> currentGeometry;
  private Map<String, Object> currentJson;
  private Tuple<SqlQuerySchema, SqlQueryColumn> currentJsonColumn;

  public FeatureEncoderSql(
      SqlQueryMapping mapping,
      EpsgCrs inputCrs,
      EpsgCrs nativeCrs,
      CrsTransformerFactory crsTransformerFactory,
      Optional<String> nullValue) {
    this.mapping = mapping;
    this.crsTransformer = crsTransformerFactory.getTransformer(inputCrs, nativeCrs);
    this.nativeCrs = nativeCrs;
    this.nullValue = nullValue;
    this.jsonMapper = new ObjectMapper();
  }

  @Modifiable
  @Value.Style(deepImmutablesDetection = true)
  interface FeatureEncoderSqlContext extends ModifiableContext<SqlQuerySchema, SqlQueryMapping> {}

  @Override
  public void onStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {}

  @Override
  public void onEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {}

  @Override
  public void onFeatureStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    currentFeature = ModifiableFeatureDataSql.create().setMapping(mapping);
    currentFeature.addRow(mapping.getMainTable());
    currentGeometry = null;
    currentJson = new LinkedHashMap<>();
    LOGGER.debug("onFeatureStart: {}", context.pathAsString());
  }

  @Override
  public void onFeatureEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    push(currentFeature);
    LOGGER.debug("onFeatureEnd: {}", context.pathAsString());

    if (currentJsonColumn != null) {
      try {
        currentFeature.addColumn(
            currentJsonColumn.first(),
            currentJsonColumn.second(),
            "'" + jsonMapper.writeValueAsString(currentJson) + "'");
      } catch (JsonProcessingException e) {
        LOGGER.error(
            "Error while serializing JSON column {}: {}",
            currentJsonColumn.second().getName(),
            e.getMessage());
      }

      mapping
          .getColumnForValue("featuretype", MappingRule.Scope.W)
          .ifPresentOrElse(
              column -> {
                currentFeature.addColumn(
                    column.first(), column.second(), "'" + mapping.getMainSchema().getName() + "'");
              },
              () -> {
                LOGGER.warn("onValue: {} not found in mapping", "featuretype");
              });
    }

    currentFeature
        .getRows()
        .forEach(
            row -> {
              LOGGER.debug("push: {} {}", row.first().getFullPathAsString(), row.second());
            });
  }

  @Override
  public void onObjectStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    LOGGER.debug("onObjectStart: {} {}", context.pathAsString(), context.inGeometry());

    if (context.inGeometry() && context.geometryType().isPresent()) {
      SimpleFeatureGeometry geometryType =
          context
              .geometryType()
              .filter(SimpleFeatureGeometry::isSpecific)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Cannot encode geometry as WKT, no specific geometry type found."));

      currentGeometry = GeometryWithStringCoordinates.of(geometryType, context.geometryDimension());
      return;
    }

    mapping.getMainSchema().getAllObjects().stream()
        .filter(schema -> Objects.equals(schema.getFullPath(), context.path()))
        .findFirst()
        .ifPresent(
            schema -> {
              LOGGER.debug("onObjectStart: {} {}", context.pathAsString(), schema);
            });
  }

  @Override
  public void onObjectEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    LOGGER.debug("onObjectEnd: {} {}", context.pathAsString(), context.inGeometry());

    if (context.inGeometry()) {
      LOGGER.debug("geometry: {} {}", context.pathAsString(), currentGeometry);

      // TODO: this is only correct for geojson, should be handled in the decoder
      mapping
          .getColumnForPrimaryGeometry()
          .ifPresentOrElse(
              column -> {
                mapping
                    .getSchemaForPrimaryGeometry()
                    .ifPresentOrElse(
                        schema -> {
                          String value = toWkt(currentGeometry, crsTransformer, nativeCrs);

                          currentFeature.addColumn(column.first(), column.second(), value);

                          LOGGER.debug("onValue: {} {}", context.pathAsString(), value);
                        },
                        () -> {
                          LOGGER.warn("onValue: {} not found in mapping", context.pathAsString());
                        });
              },
              () -> {
                LOGGER.warn("onValue: {} not found in mapping", context.pathAsString());
              });

      currentGeometry = null;
    }
  }

  @Override
  public void onArrayStart(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    LOGGER.debug("onArrayStart: {} {}", context.pathAsString(), context.inGeometry());

    if (context.inGeometry()) {
      currentGeometry.openChild();
    }
  }

  @Override
  public void onArrayEnd(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    LOGGER.debug("onArrayEnd: {} {}", context.pathAsString(), context.inGeometry());

    if (context.inGeometry()) {
      currentGeometry.closeChild();
    }
  }

  @Override
  public void onValue(ModifiableContext<SqlQuerySchema, SqlQueryMapping> context) {
    if (context.inGeometry() && Objects.nonNull(currentGeometry)) {
      currentGeometry.addCoordinate(context.value());
      LOGGER.debug(
          "onValue: {} {} {}", context.pathAsString(), context.value(), context.inGeometry());
      return;
    }

    mapping
        .getColumnForValue(context.pathAsString(), MappingRule.Scope.W)
        // TODO: this is only correct for geojson, should be handled in the decoder
        .or(() -> "id".equals(context.pathAsString()) ? mapping.getColumnForId() : Optional.empty())
        .ifPresentOrElse(
            column -> {
              if (column.second().hasOperation(Operation.CONNECTOR)) {
                if (column.second().getOperationParameter(Operation.CONNECTOR, "").equals("JSON")) {
                  if (currentJsonColumn == null) {
                    currentJsonColumn = column;
                    currentJson = new LinkedHashMap<>();
                  }
                  // TODO: sql name
                  currentJson.put(context.pathAsString(), context.value());
                }
                return;
              }

              boolean needsQuotes =
                  column.second().getType() == SchemaBase.Type.STRING
                      || column.second().getType() == SchemaBase.Type.DATETIME;
              /* (schemaSql.getType() == SchemaBase.Type.FEATURE_REF
              && schemaSql.getValueType().orElse(SchemaBase.Type.STRING)
              == SchemaBase.Type.STRING)*/

              String value =
                  needsQuotes && Objects.nonNull(context.value())
                      ? String.format("'%s'", context.value().replaceAll("'", "''"))
                      : context.value();

              if (column.second().getRole().isPresent()
                  && column.second().getRole().get() == Role.ID) {
                value = "gen_random_uuid()";
              }

              currentFeature.addColumn(column.first(), column.second(), value);

              LOGGER.debug("onValue: {} {}", context.pathAsString(), value);
            },
            () -> {
              LOGGER.warn("onValue: {} not found in mapping", context.pathAsString());
            });
  }

  @Override
  public Class<? extends ModifiableContext<SqlQuerySchema, SqlQueryMapping>> getContextInterface() {
    return FeatureEncoderSqlContext.class;
  }

  @Override
  public ModifiableContext<SqlQuerySchema, SqlQueryMapping> createContext() {
    return ModifiableFeatureEncoderSqlContext.create();
  }

  private static String toWkt(
      GeometryWithStringCoordinates<?> geometry,
      Optional<CrsTransformer> crsTransformer,
      EpsgCrs nativeCrs) {
    SimpleFeatureGeometryFromToWkt wktType =
        SimpleFeatureGeometryFromToWkt.fromSimpleFeatureGeometry(geometry.getType());
    int dimension = geometry.getDimension();

    StringWriter geometryWriter = new StringWriter();
    ImmutableCoordinatesTransformer.Builder coordinatesTransformerBuilder =
        ImmutableCoordinatesTransformer.builder();
    coordinatesTransformerBuilder.coordinatesWriter(
        ImmutableCoordinatesWriterWkt.of(geometryWriter, dimension));

    coordinatesTransformerBuilder.crsTransformer(crsTransformer);

    coordinatesTransformerBuilder.sourceDimension(dimension);
    coordinatesTransformerBuilder.targetDimension(dimension);

    geometryWriter.append(wktType.toString());

    try {
      toWktArray(geometry, geometryWriter, coordinatesTransformerBuilder);
    } catch (IOException e) {

    }

    // TODO: functions from Dialect
    String result = String.format("ST_GeomFromText('%s',%s)", geometryWriter, nativeCrs.getCode());

    if (geometry.getType() == SimpleFeatureGeometry.POLYGON
        || geometry.getType() == SimpleFeatureGeometry.MULTI_POLYGON) {
      result = String.format("ST_ForcePolygonCW(%s)", result);
    }

    return result;
  }

  // TODO: test all geo types
  private static void toWktArray(
      GeometryWithStringCoordinates<?> geometry,
      Writer structureWriter,
      Builder coordinatesWriterBuilder)
      throws IOException {
    structureWriter.append("(");

    Writer coordinatesWriter = coordinatesWriterBuilder.build();

    if (geometry instanceof GeometryWithStringCoordinates.Point point) {
      for (int i = 0; i < point.getCoordinates().size(); i++) {
        coordinatesWriter.append(point.getCoordinates().get(i));
        if (i < point.getCoordinates().size() - 1) {
          coordinatesWriter.append(",");
        }
      }

      coordinatesWriter.flush();
    } else if (geometry instanceof GeometryWithStringCoordinates.LineString lineString) {
      for (int i = 0; i < lineString.getCoordinates().size(); i++) {
        coordinatesWriter.append(lineString.getCoordinates().get(i));
        if (i < lineString.getCoordinates().size() - 1) {
          coordinatesWriter.append(",");
        }
      }

      coordinatesWriter.flush();
    } else if (geometry instanceof GeometryWithStringCoordinates.Polygon polygon) {
      for (int i = 0; i < polygon.getCoordinates().size(); i++) {
        structureWriter.append("(");

        for (int j = 0; j < polygon.getCoordinates().get(i).size(); j++) {
          coordinatesWriter.append(polygon.getCoordinates().get(i).get(j));
          if (j < polygon.getCoordinates().get(i).size() - 1) {
            coordinatesWriter.append(",");
          }
        }

        coordinatesWriter.flush();

        structureWriter.append(")");
      }
    }

    structureWriter.append(")");
  }

  // TODO: test all geo types
  private static void toWktArray(
      PropertySql propertySql, Writer structureWriter, Builder coordinatesWriterBuilder)
      throws IOException {
    if (propertySql.getType() == PropertyBase.Type.ARRAY) {
      structureWriter.append("(");
    }

    if (propertySql.getType() == PropertyBase.Type.ARRAY
        && !propertySql.getNestedProperties().isEmpty()
        && propertySql.getNestedProperties().get(0).getType() == Type.VALUE) {
      Writer coordinatesWriter = coordinatesWriterBuilder.build();

      for (int i = 0; i < propertySql.getNestedProperties().size(); i++) {
        coordinatesWriter.append(propertySql.getNestedProperties().get(i).getValue());
        if (i < propertySql.getNestedProperties().size() - 1) {
          coordinatesWriter.append(",");
        }
      }

      coordinatesWriter.flush();
    } else {
      for (int i = 0; i < propertySql.getNestedProperties().size(); i++) {
        PropertySql propertySql1 = propertySql.getNestedProperties().get(i);
        if (propertySql1.getType() == PropertyBase.Type.ARRAY) {
          toWktArray(propertySql1, structureWriter, coordinatesWriterBuilder);
          if (i < propertySql.getNestedProperties().size() - 1) {
            structureWriter.append(",");
          }
        }
      }
    }

    if (propertySql.getType() == PropertyBase.Type.ARRAY) {
      structureWriter.append(")");
    }
  }
}
